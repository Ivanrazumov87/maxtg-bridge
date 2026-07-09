"""
Потокобезопасное хранилище состояния моста MAX <-> Telegram.

Хранит:
- offset для getUpdates Telegram (чтобы не терять и не дублировать апдейты);
- окно cid собственных исходящих сообщений (защита от эхо-петли).

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
        self._tg_offset = int(data.get("tg_offset", 0))

    def _save(self):
        """Атомарная запись (через временный файл), вызывать под локом."""
        data = {
            "tg_offset": self._tg_offset,
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

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
