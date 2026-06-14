"""
Потокобезопасное хранилище состояния моста MAX <-> Telegram.

Хранит:
- соответствие max_chat_id <-> tg_topic_id (message_thread_id топика в супергруппе);
- offset для getUpdates Telegram (чтобы не терять и не дублировать апдейты);
- кэш названий чатов (для переименования топиков при необходимости).

Всё сериализуется в один JSON-файл. Доступ защищён реентерабельным локом,
т.к. обращения идут из двух потоков: обработчика MAX и поллера Telegram.
"""
import json
import os
import threading
from collections import deque


class BridgeStore:
    def __init__(self, path: str = "bridge_state.json"):
        self.path = path
        self._lock = threading.RLock()

        # cid сообщений, отправленных НАМИ в MAX из Telegram. Нужны, чтобы при
        # эхо-возврате того же сообщения по opcode 128 не переслать его обратно
        # в Telegram (защита от петли). Держим ограниченное окно последних cid.
        self._own_cids: deque = deque(maxlen=2000)
        self._own_cids_set: set[int] = set()

        # max_chat_id (str) -> tg_topic_id (int)
        self._chat_to_topic: dict[str, int] = {}
        # tg_topic_id (str) -> max_chat_id (int)
        self._topic_to_chat: dict[str, int] = {}
        # запомненные названия топиков, чтобы не дёргать переименование зря
        self._topic_titles: dict[str, str] = {}
        # offset для getUpdates
        self._tg_offset: int = 0

        self._load()

    # region _load / _save
    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            # повреждённый файл не должен ронять бота — начинаем с чистого состояния
            return
        self._chat_to_topic = {str(k): int(v) for k, v in data.get("chat_to_topic", {}).items()}
        self._topic_to_chat = {str(k): int(v) for k, v in data.get("topic_to_chat", {}).items()}
        self._topic_titles = {str(k): str(v) for k, v in data.get("topic_titles", {}).items()}
        self._tg_offset = int(data.get("tg_offset", 0))

    def _save(self):
        """Атомарная запись (через временный файл), вызывать под локом."""
        data = {
            "chat_to_topic": self._chat_to_topic,
            "topic_to_chat": self._topic_to_chat,
            "topic_titles": self._topic_titles,
            "tg_offset": self._tg_offset,
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    # region topic mapping
    def get_topic_for_chat(self, max_chat_id: int) -> int | None:
        with self._lock:
            return self._chat_to_topic.get(str(max_chat_id))

    def get_chat_for_topic(self, tg_topic_id: int) -> int | None:
        with self._lock:
            return self._topic_to_chat.get(str(tg_topic_id))

    def bind(self, max_chat_id: int, tg_topic_id: int, title: str | None = None):
        """Связывает чат MAX с топиком Telegram и сохраняет на диск."""
        with self._lock:
            self._chat_to_topic[str(max_chat_id)] = int(tg_topic_id)
            self._topic_to_chat[str(tg_topic_id)] = int(max_chat_id)
            if title is not None:
                self._topic_titles[str(tg_topic_id)] = title
            self._save()

    def get_topic_title(self, tg_topic_id: int) -> str | None:
        with self._lock:
            return self._topic_titles.get(str(tg_topic_id))

    def unbind_chat(self, max_chat_id: int):
        """Удаляет связь чата с топиком (напр. когда топик удалён в Telegram)."""
        with self._lock:
            topic = self._chat_to_topic.pop(str(max_chat_id), None)
            if topic is not None:
                self._topic_to_chat.pop(str(topic), None)
                self._topic_titles.pop(str(topic), None)
            self._save()

    # region tg offset
    @property
    def tg_offset(self) -> int:
        with self._lock:
            return self._tg_offset

    def set_tg_offset(self, offset: int):
        with self._lock:
            if offset > self._tg_offset:
                self._tg_offset = offset
                self._save()

    # region own cids (anti-echo)
    def remember_own_cid(self, cid: int):
        """Запоминает cid сообщения, которое мы сами отправили в MAX."""
        if cid is None:
            return
        with self._lock:
            if cid in self._own_cids_set:
                return
            if len(self._own_cids) == self._own_cids.maxlen:
                old = self._own_cids[0]
                self._own_cids_set.discard(old)
            self._own_cids.append(cid)
            self._own_cids_set.add(cid)

    def is_own_cid(self, cid: int) -> bool:
        """True, если это эхо нашего же исходящего сообщения."""
        if cid is None:
            return False
        with self._lock:
            return cid in self._own_cids_set
