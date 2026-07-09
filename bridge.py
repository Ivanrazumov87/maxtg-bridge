"""
Оркестратор двустороннего моста MAX <-> Telegram (режим «одна группа ⇄ одна группа»).

Одна конкретная группа MAX жёстко привязана к одной обычной группе Telegram:
- MAX -> TG: сообщение из привязанной группы MAX уходит в группу Telegram;
- TG -> MAX: сообщение из группы Telegram отправляется в группу MAX.

В обе стороны имя автора добавляется префиксом в текст («Имя: текст»), т.к. бот
в каждом мессенджере один и слать от чужого имени нельзя. Так участники видят,
кто из другого мессенджера написал — иллюзия единого чата.

Защита от эхо-петли: cid сообщений, отправленных нами в MAX из Telegram,
запоминается; их эхо (opcode 128) обратно в Telegram не пересылается.
"""
import json
import os
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

    # region MAX -> TG
    def on_max_message(self, name: str, text: str, attaches: list, max_chat_id: int, cid, silent: bool = False):
        """
        Обрабатывает новое сообщение из группы MAX и пересылает в группу Telegram.

        silent=True — отправить в Telegram без уведомления.

        Возврат True, если переслали; False — если пропустили (эхо/чужой чат).
        """
        # эхо нашего же исходящего сообщения — не пересылаем обратно
        if self.store.is_own_cid(cid):
            return False

        # интересует только привязанная группа MAX
        if int(max_chat_id) != self.max_group_id:
            return False

        caption = f"<b>{name}</b>\n{text}" if text else f"<b>{name}</b>"

        # ДИАГ: печатаем сырую структуру входящих attach, чтобы увидеть реальные
        # имена полей фото/видео от сервера MAX (для точной настройки парсинга).
        if MEDIA_DIAG and attaches:
            print("[media-diag] MAX->TG attaches:",
                  json.dumps(attaches, ensure_ascii=False))

        # ВИДЕО из MAX обрабатываем отдельно: в attach нет прямой ссылки, поэтому
        # запрашиваем URL по videoId/token и отправляем через sendVideo.
        videos = [a for a in (attaches or []) if a.get("_type") == "VIDEO"]
        other = [a for a in (attaches or []) if a.get("_type") != "VIDEO"]

        # сначала текст/фото/файлы
        if caption.strip() or other:
            telegram.send_to_telegram(
                self.tg_token, self.tg_group_id, caption, other,
                disable_notification=silent,
            )
            caption = ""  # подпись уже отправлена, не дублируем у видео

        for v in videos:
            self._forward_max_video(v, caption, silent=silent)
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
            resp = telegram.send_video(
                self.tg_token, self.tg_group_id, url,
                caption=caption, disable_notification=silent,
            )
            if resp and resp.get("ok"):
                return
            print("[bridge] sendVideo не принял URL:", resp)
        # фолбэк: уведомляем, что было видео. Сырой (приватный/подписанный) URL
        # в чат НЕ публикуем — это утечка токена и он всё равно не откроется.
        note = (caption + "\n" if caption else "") + "🎬 Видео из MAX (не удалось переслать)"
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

        # имя автора из Telegram -> префикс в текст (иллюзия единого чата)
        author = self._tg_author_name(frm)

        # текст или подпись к медиа
        text = message.get("text") or message.get("caption") or ""

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
                on_cid=self.store.remember_own_cid,
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

        return attaches, warnings

    # region telegram poller
    def telegram_poll_loop(self):
        """Фоновый long-polling приём апдейтов Telegram (TG -> MAX)."""
        print("[bridge] Telegram poller запущен")
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
