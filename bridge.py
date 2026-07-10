"""
Оркестратор двустороннего моста MAX <-> Telegram (режим «одна группа ⇄ одна группа»).

Одна конкретная группа MAX жёстко привязана к одной обычной группе Telegram:
- MAX -> TG: сообщение из привязанной группы MAX уходит в группу Telegram;
- TG -> MAX: сообщение из группы Telegram отправляется в группу MAX.

В обе стороны имя автора помечается источником: «Имя [MAX]: текст» в Telegram и
«Имя [TG]: текст» в MAX. Пометка показывает людям, из какого мессенджера пишет
человек, И служит дополнительным барьером анти-петли.

Защита от петли (многослойная):
1. Пометка источника: сообщение с «[TG]» в начале — наша пересылка, обратно не идёт.
2. Дедуп по стабильному message.id (store) — переживает рестарт, ловит повторы.
3. is_bot на TG-стороне — наши пересылки в TG идут от бота.
"""
import json
import os
import re
import threading
import time

import telegram
from store import BridgeStore

# Лимит Telegram Bot API на скачивание файлов ботом (~20 МБ). Файлы крупнее
# getFile скачать не даёт ("file is too big") — такие пропускаем с уведомлением.
TG_DOWNLOAD_LIMIT = 20 * 1024 * 1024

# Диагностика структуры входящих медиа MAX. Включается ENV MEDIA_DIAG=1 на время
# отладки: печатает сырой attach (реальные имена полей фото/видео от сервера MAX).
# По умолчанию выключено — payload содержит подписанные URL и токены.
MEDIA_DIAG = bool(os.getenv("MEDIA_DIAG"))

# Метки источника, добавляемые к имени автора при пересылке.
TAG_FROM_TG = "[TG]"    # в MAX: сообщение пришло из Telegram
TAG_FROM_MAX = "[MAX]"  # в TG: сообщение пришло из MAX

# Распознаёт нашу же пересылку из TG в MAX: имя автора помечено "[TG]" и дальше
# двоеточие. Пример совпадения: "Иван Разумов [TG]: привет". Якорим на начало
# первой строки, чтобы случайный "[TG]" внутри текста не сработал.
_OWN_FORWARD_RE = re.compile(r"^.{0,128}?\[TG\]:\s", re.DOTALL)


class Bridge:
    def __init__(self, max_client, tg_bot_token: str, tg_group_id: int,
                 max_group_id: int, store_path: str = "bridge_state.json"):
        self.max = max_client
        self.tg_token = tg_bot_token
        self.tg_group_id = tg_group_id
        # ID группы MAX, которую зеркалим (жёсткая привязка к tg_group_id)
        self.max_group_id = int(max_group_id)
        self.store = BridgeStore(store_path)

        self._stop = False

    # region анти-петля
    @staticmethod
    def is_own_forward(text: str) -> bool:
        """
        True, если текст — наша же пересылка из Telegram (имя помечено «[TG]:»).
        Такие сообщения обратно в Telegram не пересылаем (барьер анти-петли).
        """
        if not text:
            return False
        return bool(_OWN_FORWARD_RE.match(text))

    # region MAX -> TG
    def on_max_message(self, name: str, text: str, attaches: list, max_chat_id: int, silent: bool = False):
        """
        Обрабатывает новое сообщение из группы MAX и пересылает в группу Telegram.

        silent=True — отправить в Telegram без уведомления.

        Возврат True, если переслали; False — если пропустили (чужой чат).
        """
        # интересует только привязанная группа MAX
        if int(max_chat_id) != self.max_group_id:
            return False

        # имя автора помечаем источником: «Имя [MAX]»
        label = f"{name} {TAG_FROM_MAX}"
        caption = f"<b>{label}</b>\n{text}" if text else f"<b>{label}</b>"

        # ДИАГ: печатаем сырую структуру входящих attach, чтобы увидеть реальные
        # имена полей фото/видео от сервера MAX (для точной настройки парсинга).
        if MEDIA_DIAG and attaches:
            print("[media-diag] MAX->TG attaches:",
                  json.dumps(attaches, ensure_ascii=False))

        # ВИДЕО и ГОЛОСОВОЕ/АУДИО обрабатываем отдельно: в attach нет прямой
        # ссылки, URL запрашивается по id+token. Голосовое MAX присылает как
        # _type=UNSUPPORTED с полем audioId — распознаём по наличию audioId.
        videos = [a for a in (attaches or []) if a.get("_type") == "VIDEO"]
        audios = [a for a in (attaches or []) if a.get("audioId") is not None
                  and a.get("_type") != "VIDEO"]
        special = {id(a) for a in videos} | {id(a) for a in audios}
        other = [a for a in (attaches or []) if id(a) not in special]

        # сначала текст/фото/файлы
        if caption.strip() or other:
            telegram.send_to_telegram(
                self.tg_token, self.tg_group_id, caption, other,
                disable_notification=silent,
            )
            caption = ""  # подпись уже отправлена, не дублируем у медиа

        for v in videos:
            self._forward_max_video(v, caption, silent=silent)
            caption = ""

        for a in audios:
            self._forward_max_audio(a, caption, silent=silent)
            caption = ""

        return True

    # region forward MAX video
    @staticmethod
    def _extract_video_ids(attach: dict):
        """
        Достаёт (video_id, token) из входящего VIDEO-attach максимально терпимо.

        Имена полей входящего видео от сервера MAX не подтверждены на живом
        соединении, поэтому пробуем несколько вероятных ключей, а также
        вложенный под-объект (с isinstance-guard, чтобы не упасть на не-dict).
        """
        video_id = attach.get("videoId") or attach.get("id") or attach.get("movieId")
        token = attach.get("token") or attach.get("videoToken")
        if video_id is None:
            sub = attach.get("video")
            if isinstance(sub, dict):
                video_id = sub.get("videoId") or sub.get("id")
                token = token or sub.get("token")
        return video_id, token

    def _forward_max_video(self, attach: dict, caption: str, silent: bool = False):
        """Получает URL видео из MAX и отправляет его в группу Telegram."""
        video_id, token = self._extract_video_ids(attach)
        if video_id is None:
            # не смогли определить id — логируем сырой attach, чтобы понять схему
            print("[bridge] VIDEO-attach без распознанного id:",
                  json.dumps(attach, ensure_ascii=False))
        url = self.max.get_video_url(video_id, token, diag=MEDIA_DIAG) if video_id is not None else None
        if url:
            # 1) пробуем по URL — быстро, Telegram сам скачивает (если URL открыт)
            resp = telegram.send_video(
                self.tg_token, self.tg_group_id, url,
                caption=caption, disable_notification=silent,
            )
            if resp and resp.get("ok"):
                return
            print("[bridge] sendVideo по URL не принял, пробуем скачать байты:", resp)

            # 2) фолбэк: скачиваем видео сами и заливаем файлом (multipart).
            # Работает с подписанными CDN-URL, которые Telegram скачать не смог.
            content = telegram.download_url_bytes(url)
            if content:
                resp2 = telegram.send_video_bytes(
                    self.tg_token, self.tg_group_id, content,
                    caption=caption, disable_notification=silent,
                )
                if resp2 and resp2.get("ok"):
                    return
                print("[bridge] sendVideo байтами не принят:", resp2)
        # заглушка: не смогли ни по URL, ни байтами (нет URL / >50 МБ / CDN закрыт)
        note = (caption + "\n" if caption else "") + "🎬 Видео из MAX (не удалось переслать)"
        telegram.send_to_telegram(
            self.tg_token, self.tg_group_id, note,
            disable_notification=silent,
        )

    # region forward MAX audio/voice
    def _forward_max_audio(self, attach: dict, caption: str, silent: bool = False):
        """Получает URL голосового/аудио из MAX и отправляет в группу Telegram."""
        audio_id = attach.get("audioId") or attach.get("id")
        token = attach.get("token") or attach.get("audioToken")
        url = self.max.get_audio_url(audio_id, token, diag=MEDIA_DIAG) if audio_id is not None else None
        if url:
            content = telegram.download_url_bytes(url)
            if content:
                # пробуем как голосовое (ogg/opus); если формат иной — файлом
                resp = telegram.send_voice_bytes(
                    self.tg_token, self.tg_group_id, content,
                    caption=caption, disable_notification=silent,
                )
                if resp and resp.get("ok"):
                    return
                print("[bridge] sendVoice не принял, шлём документом:", resp)
                resp2 = telegram.send_document_bytes(
                    self.tg_token, self.tg_group_id, content,
                    caption=caption, filename="voice.ogg",
                    disable_notification=silent,
                )
                if resp2 and resp2.get("ok"):
                    return
                print("[bridge] sendDocument для аудио не принят:", resp2)
        else:
            # URL не получили — логируем, чтобы уточнить протокол (opcode/поля)
            print("[bridge] не удалось получить URL аудио, attach:",
                  json.dumps(attach, ensure_ascii=False))
        # заглушка
        note = (caption + "\n" if caption else "") + "🎤 Голосовое из MAX (не удалось переслать)"
        telegram.send_to_telegram(
            self.tg_token, self.tg_group_id, note,
            disable_notification=silent,
        )

    # region TG -> MAX
    def _handle_tg_message(self, message: dict):
        """Обрабатывает одно входящее сообщение Telegram -> группа MAX."""
        # сообщения от ботов игнорируем (в т.ч. собственные пересылки бота)
        frm = message.get("from", {})
        if frm.get("is_bot"):
            return

        # интересуют только сообщения из нашей группы Telegram
        chat = message.get("chat", {})
        if str(chat.get("id")) != str(self.tg_group_id):
            return

        # ДЕДУП (страховка): один и тот же message_id не пересылаем дважды
        if not self.store.seen_tg(message.get("message_id")):
            return

        # имя автора из Telegram + пометка источника: «Имя [TG]»
        author = f"{self._tg_author_name(frm)} {TAG_FROM_TG}"

        # текст или подпись к медиа
        text = message.get("text") or message.get("caption") or ""

        # голосовое/аудио уходит в MAX файлом (настоящее голосовое MAX не даёт
        # залить через открытый протокол) — помечаем текстом, чтобы было понятно
        if not text and (message.get("voice") or message.get("audio")):
            text = "🎤 голосовое"

        # собираем вложения и заливаем их в MAX; warnings — что не удалось
        attaches, warnings = self._build_max_attaches(message)

        # предупреждения (напр. слишком большой файл) шлём обратно в ту же группу
        for w in warnings:
            telegram.send_to_telegram(self.tg_token, self.tg_group_id, w)

        # служебное сообщение без текста и без вложений — пропускаем
        if not text and not attaches:
            return

        # «Имя: текст». Если текста нет (только медиа) — показываем автора отдельно.
        body = f"{author}: {text}" if text else author

        try:
            self.max.send_message(
                self.max_group_id,
                body,
                attaches=attaches or None,
            )
        except Exception as e:
            print(f"[bridge] ошибка отправки в MAX (группа {self.max_group_id}):", e)
            # сообщаем отправителю в TG, что доставка в MAX не удалась —
            # иначе он думает, что всё ушло, а на деле сообщение потеряно
            telegram.send_to_telegram(
                self.tg_token, self.tg_group_id,
                "⚠️ Сообщение не доставлено в MAX.",
            )

    # region tg author name
    @staticmethod
    def _tg_author_name(frm: dict) -> str:
        """Человекочитаемое имя автора Telegram-сообщения для префикса."""
        first = (frm.get("first_name") or "").strip()
        last = (frm.get("last_name") or "").strip()
        full = (first + " " + last).strip()
        if full:
            return full
        username = frm.get("username")
        if username:
            return f"@{username}"
        return "Без имени"

    # region build attaches
    def _build_max_attaches(self, message: dict):
        """
        Скачивает медиа из Telegram-сообщения и загружает в MAX.

        Returns:
            (attaches, warnings): список attach-элементов для send_message и
            список текстовых предупреждений (напр. про слишком большой файл),
            которые нужно отправить пользователю в группу.
        """
        attaches = []
        warnings = []

        # ФОТО: берём наибольший размер (последний в массиве photo).
        # Фото у Telegram сжатые и почти всегда влезают в лимит.
        photos = message.get("photo")
        if photos:
            file_id = photos[-1]["file_id"]
            content = telegram.download_file(self.tg_token, file_id)
            if content:
                try:
                    attaches.append(self.max.upload_photo(content))
                except Exception as e:
                    print("[bridge] не удалось загрузить фото в MAX:", e)
                    warnings.append("⚠️ Не удалось отправить фото в MAX.")
            else:
                warnings.append("⚠️ Не удалось скачать фото из Telegram.")

        # ВИДЕО (обычное видео или видео-кружочек video_note)
        video = message.get("video") or message.get("video_note")
        if video:
            size = video.get("file_size")
            if size and size > TG_DOWNLOAD_LIMIT:
                warnings.append(
                    f"⚠️ Видео не отправлено в MAX: {size // (1024*1024)} МБ — боты "
                    f"Telegram не могут скачивать файлы больше 20 МБ."
                )
            else:
                filename = video.get("file_name", "video.mp4")
                content = telegram.download_file(self.tg_token, video["file_id"])
                if content:
                    try:
                        attaches.append(self.max.upload_video(content, filename=filename))
                    except Exception as e:
                        print("[bridge] не удалось загрузить видео в MAX:", e)
                        warnings.append("⚠️ Не удалось отправить видео в MAX.")
                else:
                    # getFile отказал: чаще всего >20 МБ (file_size не всегда есть)
                    warnings.append(
                        "⚠️ Не удалось скачать видео из Telegram — вероятно, оно "
                        "больше 20 МБ (лимит ботов)."
                    )

        # ДОКУМЕНТ (любой файл, прикреплённый как файл)
        document = message.get("document")
        if document:
            size = document.get("file_size")
            if size and size > TG_DOWNLOAD_LIMIT:
                warnings.append(
                    f"⚠️ Файл не отправлен в MAX: {size // (1024*1024)} МБ — боты "
                    f"Telegram не могут скачивать файлы больше 20 МБ."
                )
            else:
                filename = document.get("file_name", "file.bin")
                content = telegram.download_file(self.tg_token, document["file_id"])
                if content:
                    try:
                        attaches.append(self.max.upload_file(content, filename=filename))
                    except Exception as e:
                        print("[bridge] не удалось загрузить файл в MAX:", e)
                        warnings.append("⚠️ Не удалось отправить файл в MAX.")
                else:
                    warnings.append(
                        "⚠️ Не удалось скачать файл из Telegram — вероятно, он "
                        "больше 20 МБ (лимит ботов)."
                    )

        # ГОЛОСОВОЕ (voice) и АУДИО (audio). Отдельного upload-метода для
        # голосового в MAX нет — заливаем файлом (в MAX появится как .ogg-файл,
        # который можно послушать). Голосовые почти всегда мелкие (< лимита).
        voice = message.get("voice")
        if voice:
            size = voice.get("file_size")
            if size and size > TG_DOWNLOAD_LIMIT:
                warnings.append("⚠️ Голосовое слишком большое для пересылки в MAX.")
            else:
                content = telegram.download_file(self.tg_token, voice["file_id"])
                if content:
                    try:
                        attaches.append(self.max.upload_file(content, filename="voice.ogg"))
                    except Exception as e:
                        print("[bridge] не удалось загрузить голосовое в MAX:", e)
                        warnings.append("⚠️ Не удалось отправить голосовое в MAX.")
                else:
                    warnings.append("⚠️ Не удалось скачать голосовое из Telegram.")

        audio = message.get("audio")
        if audio:
            size = audio.get("file_size")
            if size and size > TG_DOWNLOAD_LIMIT:
                warnings.append(
                    f"⚠️ Аудио не отправлено в MAX: {size // (1024*1024)} МБ — боты "
                    f"Telegram не могут скачивать файлы больше 20 МБ."
                )
            else:
                filename = audio.get("file_name") or (
                    f"{audio.get('performer', '')} - {audio.get('title', 'audio')}".strip(" -") + ".mp3"
                )
                content = telegram.download_file(self.tg_token, audio["file_id"])
                if content:
                    try:
                        attaches.append(self.max.upload_file(content, filename=filename))
                    except Exception as e:
                        print("[bridge] не удалось загрузить аудио в MAX:", e)
                        warnings.append("⚠️ Не удалось отправить аудио в MAX.")
                else:
                    warnings.append("⚠️ Не удалось скачать аудио из Telegram.")

        return attaches, warnings

    # region skip backlog
    def _skip_backlog_if_first_start(self):
        """
        При самом первом старте (нет сохранённого состояния) Telegram отдаёт всю
        накопленную за ~24ч очередь. Промотаем её, НЕ пересылая в MAX, чтобы
        старьё не вывалилось задним числом. Выполняется один раз за жизнь моста.
        """
        if self.store.initialized:
            return
        print("[bridge] первый старт — проматываем накопленную очередь Telegram")
        while not self._stop:
            updates = telegram.get_updates(
                self.tg_token, offset=self.store.tg_offset, timeout=0,
            )
            if not updates:
                break
            self.store.set_tg_offset(updates[-1]["update_id"] + 1)
        self.store.mark_initialized()

    def telegram_poll_loop(self):
        """Фоновый long-polling приём апдейтов Telegram (TG -> MAX)."""
        print("[bridge] Telegram poller запущен")
        self._skip_backlog_if_first_start()
        while not self._stop:
            try:
                updates = telegram.get_updates(
                    self.tg_token,
                    offset=self.store.tg_offset,
                    timeout=30,
                )
                for upd in updates:
                    self.store.set_tg_offset(upd["update_id"] + 1)
                    message = upd.get("message")
                    if message:
                        self._handle_tg_message(message)
            except Exception as e:
                print("[bridge] ошибка Telegram poller:", e)
                time.sleep(3)

    def start_telegram_poller(self):
        t = threading.Thread(target=self.telegram_poll_loop, name="TelegramPoller", daemon=True)
        t.start()
        return t

    def stop(self):
        self._stop = True
