"""
Потокобезопасное хранилище состояния моста MAX <-> Telegram.

Хранит:
- offset для getUpdates Telegram (чтобы не терять и не дублировать апдейты);
- окна уже пересланных ID сообщений (дедупликация в обе стороны, защита от
  повторной доставки сервером и от гонки воркеров);
- флаг initialized — был ли уже пройден первый старт (для промотки очереди TG).

Всё сериализуется в один JSON-файл. Доступ защищён реентерабельным локом,
т.к. обращения идут из нескольких потоков: воркеров обработки MAX (пул) и
поллера Telegram.
"""
import json
import os
import threading
from collections import deque

# Сколько последних ID сообщений помнить для дедупликации в каждую сторону.
# Окно нужно только чтобы отсечь ПОВТОРНУЮ доставку недавних сообщений (эхо,
# ре-доставка сервером после реконнекта, гонка воркеров). Старые ID сервер
# повторно не присылает, поэтому большого окна не требуется. 5000 ~ десятки КБ.
SEEN_MAXLEN = 5000


class BridgeStore:
    def __init__(self, path: str = "bridge_state.json"):
        self.path = path
        self._lock = threading.RLock()

        # offset для getUpdates
        self._tg_offset: int = 0

        # Окна уже пересланных ID (строки — нормализуем тип, чтобы id из JSON
        # после рестарта матчился с id входящего). deque хранит порядок для
        # вытеснения старых, set — для O(1) проверки.
        self._seen_max: deque = deque(maxlen=SEEN_MAXLEN)   # message.id из MAX
        self._seen_max_set: set[str] = set()
        self._seen_tg: deque = deque(maxlen=SEEN_MAXLEN)     # message_id из Telegram
        self._seen_tg_set: set[str] = set()

        # был ли уже первый старт (после него не проматываем очередь TG заново)
        self._initialized: bool = False

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
        self._initialized = bool(data.get("initialized", False))
        for k in data.get("seen_max_ids", []):
            self._seen_max.append(str(k))
            self._seen_max_set.add(str(k))
        for k in data.get("seen_tg_ids", []):
            self._seen_tg.append(str(k))
            self._seen_tg_set.add(str(k))

    def _save(self):
        """Атомарная запись (через временный файл), вызывать под локом."""
        data = {
            "tg_offset": self._tg_offset,
            "initialized": self._initialized,
            "seen_max_ids": list(self._seen_max),
            "seen_tg_ids": list(self._seen_tg),
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

    # region initialized
    @property
    def initialized(self) -> bool:
        with self._lock:
            return self._initialized

    def mark_initialized(self):
        with self._lock:
            if not self._initialized:
                self._initialized = True
                self._save()

    # region дедупликация (атомарный check-and-add)
    def _check_and_add(self, dq: deque, st: set, key) -> bool:
        """
        Атомарно: если key ещё не встречался — запомнить и вернуть True (новое,
        надо переслать). Если уже был — вернуть False (дубль, пропустить).

        key=None считаем «новым» (переслать один раз), но в окно НЕ кладём —
        иначе первый None заглушил бы все последующие сообщения без id.
        Проверка и добавление в одном методе под локом — чтобы параллельные
        воркеры MAX не переслали одно сообщение дважды.
        """
        if key is None:
            return True
        key = str(key)
        with self._lock:
            if key in st:
                return False
            if len(dq) == dq.maxlen:
                st.discard(dq[0])   # deque сам вытеснит dq[0] при append
            dq.append(key)
            st.add(key)
            self._save()
            return True

    def seen_max(self, msg_id) -> bool:
        """True, если сообщение MAX новое (надо переслать); False — дубль."""
        return self._check_and_add(self._seen_max, self._seen_max_set, msg_id)

    def seen_tg(self, msg_id) -> bool:
        """True, если сообщение Telegram новое (надо переслать); False — дубль."""
        return self._check_and_add(self._seen_tg, self._seen_tg_set, msg_id)
