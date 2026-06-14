"""
Оркестратор двустороннего моста MAX <-> Telegram (режим форум-топиков).

Каждый чат MAX отображается в отдельный топик форум-супергруппы Telegram:
- MAX -> TG: новое сообщение из чата MAX уходит в соответствующий топик
  (топик создаётся автоматически при первом сообщении);
- TG -> MAX: сообщение, написанное в топик, отправляется в привязанный чат MAX.

Защита от эхо-петли: cid сообщений, отправленных нами в MAX из Telegram,
запоминается; их эхо (opcode 128) обратно в Telegram не пересылается.
"""
import threading
import time

import telegram
from store import BridgeStore

# Лимит Telegram Bot API на скачивание файлов ботом (~20 МБ). Файлы крупнее
# getFile скачать не даёт ("file is too big") — такие пропускаем с уведомлением.
TG_DOWNLOAD_LIMIT = 20 * 1024 * 1024


class Bridge:
    def __init__(self, max_client, tg_bot_token: str, tg_group_id: int,
                 store_path: str = "bridge_state.json"):
        self.max = max_client
        self.tg_token = tg_bot_token
        self.tg_group_id = tg_group_id
        self.store = BridgeStore(store_path)

        # сериализует создание топиков, чтобы не плодить дубликаты при
        # одновременном приходе нескольких сообщений из нового чата
        self._topic_lock = threading.Lock()
        self._stop = False

    # region ensure_topic
    def ensure_topic(self, max_chat_id: int) -> int | None:
        """
        Возвращает message_thread_id топика для чата MAX, создавая его при
        необходимости. None — если топик не удалось создать.
        """
        topic = self.store.get_topic_for_chat(max_chat_id)
        if topic is not None:
            return topic

        with self._topic_lock:
            # повторная проверка под локом (могли создать, пока ждали лок)
            topic = self.store.get_topic_for_chat(max_chat_id)
            if topic is not None:
                return topic

            title = self.max.get_chat_title(max_chat_id)
            topic = telegram.create_forum_topic(self.tg_token, self.tg_group_id, title)
            if topic is None:
                return None
            self.store.bind(max_chat_id, topic, title)
            print(f"[bridge] создан топик '{title}' (thread {topic}) для чата MAX {max_chat_id}")
            return topic

    # region MAX -> TG
    def on_max_message(self, name: str, text: str, attaches: list, max_chat_id: int, cid):
        """
        Обрабатывает новое сообщение из MAX и пересылает в нужный топик Telegram.

        Возврат True, если переслали; False — если пропустили (эхо/ошибка).
        """
        # эхо нашего же исходящего сообщения — не пересылаем обратно
        if self.store.is_own_cid(cid):
            return False

        topic = self.ensure_topic(max_chat_id)
        if topic is None:
            print(f"[bridge] нет топика для чата {max_chat_id}, сообщение пропущено")
            return False

        caption = f"<b>{name}</b>\n{text}" if text else f"<b>{name}</b>"

        # ВИДЕО из MAX обрабатываем отдельно: в attach нет прямой ссылки, поэтому
        # запрашиваем URL по videoId/token и отправляем через sendVideo.
        videos = [a for a in (attaches or []) if a.get("_type") == "VIDEO"]
        other = [a for a in (attaches or []) if a.get("_type") != "VIDEO"]

        # сначала текст/фото/файлы — с авто-пересозданием темы, если её удалили
        if caption.strip() or other:
            resp = telegram.send_to_telegram(
                self.tg_token, self.tg_group_id, caption, other,
                message_thread_id=topic,
            )
            if self._topic_gone(resp):
                topic = self._recreate_topic(max_chat_id)
                if topic is not None:
                    telegram.send_to_telegram(
                        self.tg_token, self.tg_group_id, caption, other,
                        message_thread_id=topic,
                    )
            caption = ""  # подпись уже отправлена, не дублируем у видео

        for v in videos:
            self._forward_max_video(v, topic, caption)
            caption = ""

        return True

    # region topic-gone helpers
    @staticmethod
    def _topic_gone(resp: dict) -> bool:
        """True, если Telegram ответил, что топик удалён/не найден/закрыт."""
        if not resp or resp.get("ok"):
            return False
        desc = (resp.get("description") or "").lower()
        return ("thread not found" in desc or "topic_deleted" in desc
                or "topic was deleted" in desc or "topic is closed" in desc
                or "message thread not found" in desc)

    def _recreate_topic(self, max_chat_id: int) -> int | None:
        """Пересоздаёт тему для чата MAX (старая удалена в Telegram)."""
        with self._topic_lock:
            self.store.unbind_chat(max_chat_id)
            title = self.max.get_chat_title(max_chat_id)
            topic = telegram.create_forum_topic(self.tg_token, self.tg_group_id, title)
            if topic is None:
                return None
            self.store.bind(max_chat_id, topic, title)
            print(f"[bridge] тема была удалена — пересоздана '{title}' (thread {topic}) для чата {max_chat_id}")
            return topic

    # region forward MAX video
    def _forward_max_video(self, attach: dict, topic: int, caption: str):
        """Получает URL видео из MAX и отправляет его в тему Telegram."""
        url = self.max.get_video_url(attach.get("videoId"), attach.get("token"))
        if url:
            resp = telegram.send_video(
                self.tg_token, self.tg_group_id, url,
                caption=caption, message_thread_id=topic,
            )
            if resp and resp.get("ok"):
                return
            print("[bridge] sendVideo не принял URL:", resp)
        # фолбэк: хотя бы уведомим, что было видео (и дадим ссылку, если есть)
        note = (caption + "\n" if caption else "") + "🎬 Видео из MAX"
        if url:
            note += f"\n{url}"
        telegram.send_to_telegram(
            self.tg_token, self.tg_group_id, note, message_thread_id=topic
        )

    # region TG -> MAX
    def _handle_tg_message(self, message: dict):
        """Обрабатывает одно входящее сообщение Telegram (из топика) -> MAX."""
        # сообщения от ботов игнорируем (в т.ч. собственные пересылки бота)
        frm = message.get("from", {})
        if frm.get("is_bot"):
            return

        # интересуют только сообщения из нашей супергруппы
        chat = message.get("chat", {})
        if str(chat.get("id")) != str(self.tg_group_id):
            return

        thread_id = message.get("message_thread_id")
        if thread_id is None:
            # сообщение вне топика (General) — некуда маршрутизировать
            return

        max_chat_id = self.store.get_chat_for_topic(thread_id)
        if max_chat_id is None:
            # топик не привязан к чату MAX (например, создан вручную) — игнор
            return

        # текст или подпись к медиа
        text = message.get("text") or message.get("caption") or ""

        # собираем вложения и заливаем их в MAX; warnings — что не удалось
        attaches, warnings = self._build_max_attaches(message)

        # предупреждения (напр. слишком большой файл) шлём обратно в ту же тему
        for w in warnings:
            telegram.send_to_telegram(
                self.tg_token, self.tg_group_id, w, message_thread_id=thread_id
            )

        # служебное сообщение без текста и без вложений — пропускаем
        if not text and not attaches:
            return

        try:
            self.max.send_message(
                max_chat_id,
                text,
                on_cid=self.store.remember_own_cid,
                attaches=attaches or None,
            )
        except Exception as e:
            print(f"[bridge] ошибка отправки в MAX (чат {max_chat_id}):", e)

    # region build attaches
    def _build_max_attaches(self, message: dict):
        """
        Скачивает медиа из Telegram-сообщения и загружает в MAX.

        Returns:
            (attaches, warnings): список attach-элементов для send_message и
            список текстовых предупреждений (напр. про слишком большой файл),
            которые нужно отправить пользователю в тему.
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

        # ВИДЕО (обычное видео или видео-кружочек video_note)
        video = message.get("video") or message.get("video_note")
        if video:
            size = video.get("file_size")
            if size and size > TG_DOWNLOAD_LIMIT:
                warnings.append(
                    f"⚠️ Видео не отправлено в MAX: размер {size // (1024*1024)} МБ "
                    f"превышает лимит Telegram-ботов (20 МБ)."
                )
            else:
                filename = video.get("file_name", "video.mp4")
                content = telegram.download_file(self.tg_token, video["file_id"])
                if content:
                    try:
                        attaches.append(self.max.upload_video(content, filename=filename))
                    except Exception as e:
                        print("[bridge] не удалось загрузить видео в MAX:", e)
                else:
                    warnings.append("⚠️ Не удалось скачать видео из Telegram (возможно, слишком большое).")

        # ДОКУМЕНТ (любой файл, прикреплённый как файл)
        document = message.get("document")
        if document:
            size = document.get("file_size")
            if size and size > TG_DOWNLOAD_LIMIT:
                warnings.append(
                    f"⚠️ Файл не отправлен в MAX: размер {size // (1024*1024)} МБ "
                    f"превышает лимит Telegram-ботов (20 МБ)."
                )
            else:
                filename = document.get("file_name", "file.bin")
                content = telegram.download_file(self.tg_token, document["file_id"])
                if content:
                    try:
                        attaches.append(self.max.upload_file(content, filename=filename))
                    except Exception as e:
                        print("[bridge] не удалось загрузить файл в MAX:", e)
                else:
                    warnings.append("⚠️ Не удалось скачать файл из Telegram (возможно, слишком большой).")

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
