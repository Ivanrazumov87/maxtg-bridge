from websockets.sync.client import connect
from websockets.exceptions import ConnectionClosedError
import json
import threading
import websockets
import time
import requests
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from classes import *
from errors import *

# HTTP-заголовки для загрузки файлов в MAX (как у веб-клиента)
_UPLOAD_HEADERS = {
    "Origin": "https://web.max.ru",
    "Referer": "https://web.max.ru/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
}

# region class MaxClient
class MaxClient:
    def __init__(self, token: str|None = None, phone: str|None = None):
        """
        Initializes a new instance of the MaxClient class.

        This constructor sets up the client with optional authentication token and phone number.
        It prepares internal state for sequence numbering, user agent generation, WebSocket connection,
        and event handlers.

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")
            # Now you can use client methods like connect(), auth(), etc.
            ```
        """

        # print("Loaded WebMaxLib")

        self._seq = 0
        self._seq_lock = threading.Lock()        # атомарный инкремент seq

        self.phone_number = phone
        self.auth_token = token
        self.user_agent = self._generate_user_agent()

        self.websocket = None
    
        self._19_payload = None
        self._on_connect = None
        self._connected = False
        self._t = None
        self._t_stop = False

        self.is_log_in = False
        self.me = None
        self.session_id = int(time.time()*1000)

        self.handlers = []

        # Потокобезопасность: единственный читатель сокета — _listener.
        # Запросы регистрируют свой seq в _pending и ждут ответ через Event.
        self._send_lock = threading.Lock()      # сериализует запись в сокет
        self._pending = {}                       # seq -> {"event": Event, "response": dict}
        self._pending_lock = threading.Lock()    # защищает _pending
        # эпоха соединения: растёт при каждом (ре)коннекте. Heartbeat привязан к
        # своей эпохе и завершается, как только соединение пересоздано — это
        # гарантирует ровно один активный heartbeat и отсутствие записи в чужой сокет.
        self._conn_epoch = 0

        # Кэш чатов и контактов, приходящий при логине (opcode 19).
        self.chats = {}                          # chat_id -> raw chat dict
        self.contacts = {}                       # contact_id -> raw contact dict
        self._title_cache = {}                   # chat_id -> готовое название (мемоизация)

        # Ожидание готовности загруженных файлов/видео: сервер присылает
        # push opcode 136 с fileId/videoId, когда вложение обработано.
        self._upload_waiters = {}                # key (str) -> Event
        self._upload_waiters_lock = threading.Lock()

        # Пул воркеров для обработки входящих сообщений. КРИТИЧНО: обработчик
        # может делать блокирующие запросы (get_user/история чата), а доставить
        # ответ способен только listener-поток. Если запускать обработку прямо
        # в listener, он заблокирует сам себя (deadlock). Поэтому обработка
        # каждого входящего уходит в отдельный воркер, listener остаётся свободен.
        self._worker_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="MaxWorker")

    # region seq
    @property
    def seq(self):
        with self._seq_lock:
            current_seq = self._seq
            self._seq += 1
            return current_seq
    
    # region cid
    @property
    def cid(self):
        return int(time.time() * 1000)
    
    # region marker
    @property
    def marker(self):
        return int("900"+str(int(time.time())))

    # region _generate_user_agent()
    def _generate_user_agent(self) -> str:
        return json.dumps({
            "ver": 11,
            "cmd": 0,
            "seq": self.seq,
            "opcode": 6,
            "payload": {
                "userAgent": {
                    "deviceType": "WEB",
                    "locale": "en",
                    "osVersion": "Windows",
                    "deviceName": "WebMax Lib",
                    "headerUserAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
                    "deviceLocale": "en",
                    "appVersion": "4.8.42",
                    "screen": "1920x1080 1.0x",
                    "timezone": "UTC"
                },
                "deviceId": str(uuid4())
            }
        })

    # region connect()
    def connect(self, _f=None, _reconnect=False):
        """
        Establishes a WebSocket connection to the server.

        This method connects to the WebSocket endpoint, sends the user agent, and authenticates using the token.
        It sets the client to connected state and retrieves the user profile.

        При _reconnect=True повторно вызывается из listener после обрыва —
        в этом случае колбэк on_connect НЕ дёргается повторно (чтобы не
        запускать Telegram-поллер второй раз).

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")
            client.connect()
            # Call this after setting the auth_token to establish the connection.
            ```
        """
        if self._connected:
            return
        headers = [
            ("Origin", "https://web.oneme.ru"),
            ("Pragma", "no-cache"),
            ("Cache-Control", "no-cache"),
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36")
        ]
        # ВАЖНО: ping_interval=None отключает встроенный WS-keepalive библиотеки.
        # MAX не отвечает на низкоуровневый WS-Ping (у него свой прикладной
        # heartbeat — opcode 1), поэтому дефолтный keepalive (ping каждые 20с +
        # ожидание Pong) ложно рвёт соединение с "no close frame", особенно когда
        # воркер занят обработкой входящего. Свой heartbeat (_heartbeat) держит
        # соединение сам.
        self.websocket = connect(
            "wss://ws-api.oneme.ru/websocket",
            additional_headers=headers,
            ping_interval=None,
            ping_timeout=None,
            open_timeout=20,
        )
        self.websocket.send(self.user_agent)
        self.websocket.recv()

        if _f:
            return

        # Логин выполняется ДО старта фонового listener, поэтому читаем сокет
        # синхронно здесь (механизм _send_and_wait ещё не обслуживается).
        login_seq = self.seq
        self.websocket.send(json.dumps({
            "ver": 11,
            "cmd": 0,
            "seq": login_seq,
            "opcode": 19,
            "payload": {
                "interactive": True,
                "token": self.auth_token,
                "chatsSync": 0,
                "contactsSync": 0,
                "presenceSync": 0,
                "draftsSync": 0,
                "chatsCount": 100
            }
        }))

        # Сервер может прислать промежуточные пакеты (напр. opcode 6) перед
        # ответом на логин — читаем, пока не получим именно ответ opcode 19.
        login_resp = None
        for _ in range(10):
            login_resp = json.loads(self.websocket.recv())
            if login_resp.get("opcode") == 19:
                break

        p = login_resp.get("payload", {})
        if "error" in p:
            raise ValueError(
                f"Логин MAX отклонён: {p.get('error')} "
                f"({p.get('localizedMessage') or p.get('message') or 'нет описания'}). "
                "Проверьте/обновите MAX_TOKEN."
            )
        if "profile" not in p:
            raise ValueError(
                f"Неожиданный ответ логина MAX (opcode {login_resp.get('opcode')}). "
                f"Ключи: {list(p.keys())}"
            )

        usr = User(self, p['profile'])
        self.me = usr

        # Сохраняем чаты и контакты из ответа логина — они нужны для
        # именования топиков Telegram и резолва имён собеседников.
        self._cache_sync(p)

        self._connected = True

        # колбэк только при первом подключении, не при автопереподключении
        if self._on_connect and not _reconnect:
            self._on_connect()

    # region disconnect()
    def disconnect(self):
        """
        Closes the WebSocket connection and resets the client state.

        This method safely disconnects from the server and resets internal flags and sequence.

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")
            client.disconnect()
            # Call this to cleanly close the connection when done.
            ```
        """
        if not self._connected:
            return
        if self.websocket:
            self.websocket.close()
            self._seq = 0
        self._connected = False
        self.websocket = None

    # region set_token()
    def set_token(self, token):
        """
        Sets the authentication token for the client.

        This updates the auth_token used for connecting to the server.

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")
            client.set_token("new_auth_token")
            # Use this to update the token before connecting or reconnecting.
            ```
        """
        self.auth_token = token

    # region _raw_send()
    def _raw_send(self, data: str):
        """Потокобезопасная запись в сокет. Все отправки идут через этот метод."""
        with self._send_lock:
            self.websocket.send(data)

    # region _send_and_wait()
    def _send_and_wait(self, opcode: int, payload: dict, timeout: float = 15.0) -> dict:
        """
        Отправляет запрос и блокирующе ждёт ответ с тем же seq.

        Ответ доставляет фоновый _listener (единственный читатель сокета) через
        механизм _pending, поэтому метод безопасно вызывать из любого потока,
        в том числе из обработчика Telegram-сообщений.

        Returns:
            dict: полный пакет ответа (recv) с совпавшим seq.

        Raises:
            TimeoutError: если ответ не пришёл за timeout секунд.
        """
        seq = self.seq
        event = threading.Event()
        slot = {"event": event, "response": None}
        with self._pending_lock:
            self._pending[seq] = slot

        try:
            self._raw_send(json.dumps({
                "ver": 11,
                "cmd": 0,
                "seq": seq,
                "opcode": opcode,
                "payload": payload
            }))
            if not event.wait(timeout):
                raise TimeoutError(f"Нет ответа на opcode {opcode} (seq {seq}) за {timeout}s")
            return slot["response"]
        finally:
            with self._pending_lock:
                self._pending.pop(seq, None)

    # region _handle_incoming()
    def _handle_incoming(self, payload):
        """
        Обрабатывает входящее сообщение (opcode 128) в воркер-потоке.

        Здесь можно безопасно делать блокирующие запросы (get_user, история
        чата), потому что мы НЕ в listener-потоке — listener свободен и доставит
        ответы через _send_and_wait.
        """
        try:
            # _f=1: не грузим историю чата на горячем пути
            msg = Message(self, payload["chatId"], **payload["message"], _f=1)
            self._hlprocessor(msg)
        except Exception as e:
            print("Ошибка обработки сообщения:", e)

    # region _hlprocessor()
    def _hlprocessor(self, msg: Message):
        """Internal worker. Don't touch."""
        for filter, func in self.handlers:
            if filter(self, msg):
                func(self, msg)
                return

    def _start_heartbeat(self):
        """Инкрементирует эпоху и запускает ровно один heartbeat-поток для неё.
        Старые heartbeat-потоки (другой эпохи) сами завершатся."""
        self._conn_epoch += 1
        epoch = self._conn_epoch
        threading.Thread(target=self._heartbeat, args=(epoch,),
                         name="WebMaxHeartbeat", daemon=True).start()

    def _heartbeat(self, epoch=None):
        """Отправляет пинг серверу каждые 25 секунд (привязан к эпохе соединения)."""
        while self._connected and not self._t_stop:
            # если соединение пересоздано (сменилась эпоха) — этот heartbeat
            # устарел и завершается, чтобы не писать в новый сокет
            if epoch is not None and epoch != self._conn_epoch:
                return
            try:
                self._raw_send(json.dumps({
                    "ver": 11,
                    "cmd": 0,
                    "seq": self.seq,
                    "opcode": 1,
                    "payload": {"interactive": False}
                }))
            except Exception as e:
                print("Heartbeat error:", e)
            # спим короткими интервалами, чтобы быстро реагировать на смену эпохи
            for _ in range(25):
                if self._t_stop or (epoch is not None and epoch != self._conn_epoch):
                    return
                time.sleep(1)


    # region _listener()
    def _listener(self):
        """Listener with batch processing for multiple messages"""
        while not self._t_stop:
            try:
                # Получаем первое сообщение
                recv = json.loads(self.websocket.recv())

                # Обрабатываем первое сообщение
                self._process_message(recv)

                # Проверяем, есть ли еще сообщения в буфере
                while True:
                    try:
                        # Пытаемся получить следующее сообщение с таймаутом 0.01 сек
                        next_msg = json.loads(self.websocket.recv(timeout=0.01))
                        self._process_message(next_msg)
                    except TimeoutError:
                        # Больше нет сообщений в буфере
                        break
                    except ConnectionClosedError:
                        raise

            except (ConnectionClosedError, OSError) as e:
                # Соединение оборвалось — пытаемся переподключиться, НЕ завершая
                # listener (он продолжит читать новый сокет).
                print("Соединение с MAX потеряно:", e)
                if not self._reconnect():
                    break  # остановлены извне (_t_stop)

            except Exception as e:
                # Прочие ошибки: если сокет жив — продолжаем, иначе реконнект
                print("Ошибка listener:", e)
                if self._t_stop:
                    break
                time.sleep(2)
                continue

    # region _reconnect()
    def _reconnect(self) -> bool:
        """
        Переподключается к MAX после обрыва. Чистит состояние и повторяет
        попытки с нарастающей паузой. Возвращает True при успехе, False если
        клиент остановлен (_t_stop).
        """
        # сбрасываем зависшие ожидания ответов — на новом соединении seq другие
        with self._pending_lock:
            for slot in self._pending.values():
                slot["event"].set()
            self._pending.clear()

        self._connected = False
        try:
            if self.websocket:
                self.websocket.close()
        except Exception:
            pass
        self.websocket = None

        delay = 3
        attempt = 0
        while not self._t_stop:
            time.sleep(delay)
            attempt += 1
            try:
                # гарантируем, что старый сокет закрыт перед новой попыткой
                try:
                    if self.websocket:
                        self.websocket.close()
                except Exception:
                    pass
                self.websocket = None
                self._connected = False

                self.connect(_reconnect=True)
                print(f"Переподключение к MAX успешно (попытка {attempt})")
                # поднимаем новый heartbeat (старый сам завершится по смене эпохи)
                self._start_heartbeat()
                return True
            except Exception as ee:
                # после серии неудач — длинная пауза (часто причина в том, что
                # та же сессия MAX используется в браузере и перебивает бота)
                if attempt >= 5:
                    delay = 60
                else:
                    delay = min(delay + 5, 30)
                print(f"Не смог встать (попытка {attempt}, следующая через {delay}с): {ee}")
        return False

    def _process_message(self, recv):
        """Process a single message"""
        opcode = recv.get("opcode")
        payload = recv.get("payload")
        seq = recv.get("seq")

        # Это ответ на чей-то запрос (_send_and_wait)? Доставляем и будим ожидающего.
        with self._pending_lock:
            slot = self._pending.get(seq)
        if slot is not None:
            slot["response"] = recv
            slot["event"].set()
            return

        match opcode:
            case 1:
                try:
                    self._raw_send(json.dumps({
                        "ver": 11,
                        "cmd": 0,
                        "seq": self.seq,
                        "opcode": 1,
                        "payload": {"interactive": False}
                    }))
                except:
                    pass

            case 128:
                # Обработку уносим в воркер, чтобы listener не блокировался на
                # вложенных запросах (get_user и т.п.) и мог доставлять ответы.
                self._worker_pool.submit(self._handle_incoming, payload)

            case 136:
                # вложение (файл/видео) обработано сервером — будим ожидающего
                key = None
                if payload and "fileId" in payload:
                    key = f"file:{payload['fileId']}"
                elif payload and "videoId" in payload:
                    key = f"video:{payload['videoId']}"
                if key is not None:
                    with self._upload_waiters_lock:
                        ev = self._upload_waiters.get(key)
                    if ev is not None:
                        ev.set()

            case _:
                pass

        # Необязательно: можно закомментировать, если спамит в консоль
        # print(json.dumps(recv, ensure_ascii=False, indent=4))


    # region run()
    def run(self):
        """
        Starts the client by connecting and launching the listener thread.

        This connects to the server and begins listening for messages in a background thread.

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")
            client.run()
            ```
        """
        self.connect()
        self._t = threading.Thread(target=self._listener, name="WebMaxListener")
        self._t.daemon = True  # Добавляем daemon=True для автоматического завершения
        self._t.start()
        self._start_heartbeat()
    
    def stop(self):
        """
        Stops the listener thread and disconnects from the server.

        This signals the listener to stop and closes the connection.

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")

            @client.on_connect # Using onconnect decorator
            def onconnect():
                client.stop() # Stops client after run

            client.run()
            ```
        """
        self._t_stop = True
        self.disconnect()

    # region _start_auth()
    def _start_auth(self, phone_number) -> dict:
        """
        Initiates the authentication process by sending a phone number to receive a verification code.

        This sends a request to start authentication and returns the server response.

        Usage:
            ```
            # You can use only token or only phone if have one.
            client = MaxClient(token="token", phone="number")
            response = client._start_auth("your_phone_number")
            ```
        """
        self.connect(_f=1)
        if self.is_log_in:
            raise ValueError("Client is logged in now")
        
        self.websocket.send(json.dumps({
            "ver": 11,
            "cmd": 0,
            "seq": self.seq,
            "opcode": 17,
            "payload": {
                "phone": phone_number,
                "type": "START_AUTH",
                "language": "ru"
            }
        }))

        return json.loads(self.websocket.recv()) # experimental
    
    # region _check_code()
    def _check_code(self, token, code) -> dict:
        self.websocket.send(json.dumps({
            "ver": 11,
            "cmd": 0,
            "seq": self.seq,
            "opcode": 18,
            "payload": {
                "token": token,
                "verifyCode": code,
                "authTokenType": "CHECK_CODE"
            }
        }))

        token_resp = json.loads(self.websocket.recv())
        payload = token_resp['payload']
        error = token_resp['payload'].get("error", None)

        if error == "verify.code.wrong":
            raise VerifyCodeWrong(payload["error"], payload["title"])
        return token_resp

    # region auth()
    def auth(self, phone_number: str):
        """
        Performs the full authentication process interactively.

        This connects, starts auth, prompts for the code, verifies it, and sets the auth_token.
        Returns the User object for the authenticated user.

        Usage:
        ```
        user = client.auth("+7xxxxxxxxxx")
        # Follow the prompt to enter the SMS code.
        ```
        """

        code_resp = self._start_auth(phone_number)

        if code_resp.get('payload', {}).get('error'):
            raise ValueError(code_resp['payload']['error'] + ": " + code_resp['payload']['localizedMessage'])
            
        token = code_resp['payload']['token']
        print(f"Auth token received. Please enter the code sent to your phone.\n")

        while True:
            try:
                code = input("Auth code: ")
                token_resp = self._check_code(token, code)

                payload = token_resp['payload']
                break

            except VerifyCodeWrong as vcw:
                print(f"{vcw.title} ({vcw.error})")
                continue

            except Exception as e:
                print(e)
                continue

        self.auth_token = payload['tokenAttrs']['LOGIN']['token']
        usr = User(self, payload['profile'])
        self.me = usr
        return self.me

    # region get_chats()
    # DONT USE THIS! BROKEN
    # def get_chats(self, count = 40) -> dict:
    #     if not self.auth_token:
    #         raise ValueError("No auth token provided. Please authenticate first.")

    #     self.websocket.send(json.dumps({
    #         "ver": 11,
    #         "cmd": 0,
    #         "seq": self.seq,
    #         "opcode": 19,
    #         "payload": {
    #             "interactive": True,
    #             "token": self.auth_token,
    #             "chatsSync": 0,
    #             "contactsSync": 0,
    #             "presenceSync": 0,
    #             "draftsSync": 0,
    #             "chatsCount": count
    #         }
    #     }))

    #     response = json.loads(self.websocket.recv())
    #     return response

    # region upload_photo()
    def upload_photo(self, content: bytes, filename: str = "image.jpg",
                     content_type: str = "image/jpeg") -> dict:
        """
        Загружает фото в MAX и возвращает attach-элемент для send_message.

        Процесс: opcode 80 (запрос upload-URL) -> multipart POST -> photoToken.

        Returns:
            dict вида {"_type": "PHOTO", "photoToken": "<token>"}.

        Raises:
            RuntimeError: если сервер не вернул URL или токен.
        """
        recv = self._send_and_wait(80, {"count": 1})
        url = recv["payload"].get("url")
        if not url:
            raise RuntimeError(f"MAX не вернул URL загрузки фото: {recv['payload']}")

        resp = requests.post(
            url,
            headers=_UPLOAD_HEADERS,
            files={"file": (filename, content, content_type)},
            timeout=60,
        )
        obj = resp.json()
        photos = obj.get("photos") or {}
        if not photos:
            raise RuntimeError(f"Ответ загрузки фото без photos: {obj}")
        token = next(iter(photos.values())).get("token")
        if not token:
            raise RuntimeError(f"В ответе загрузки фото нет token: {obj}")
        return {"_type": "PHOTO", "photoToken": token}

    # region upload_file()
    def upload_file(self, content: bytes, filename: str = "file.bin",
                    wait_timeout: float = 60.0) -> dict:
        """
        Загружает произвольный файл в MAX и возвращает attach-элемент.

        Процесс: opcode 87 (запрос upload-URL) -> сырой POST с Content-Range ->
        ожидание серверного push opcode 136 (файл обработан) -> attach по fileId.

        Returns:
            dict вида {"_type": "FILE", "fileId": <int>}.

        Raises:
            RuntimeError: если сервер не вернул URL/fileId.
            TimeoutError: если push готовности не пришёл за wait_timeout.
        """
        recv = self._send_and_wait(87, {"count": 1})
        info_list = recv["payload"].get("info") or []
        if not info_list:
            raise RuntimeError(f"MAX не вернул info для загрузки файла: {recv['payload']}")
        info = info_list[0]
        url = info.get("url")
        file_id = info.get("fileId")
        if not url or file_id is None:
            raise RuntimeError(f"Неполный ответ загрузки файла: {info}")

        # регистрируем ожидание готовности ДО POST, чтобы не пропустить push 136
        key = f"file:{file_id}"
        event = threading.Event()
        with self._upload_waiters_lock:
            self._upload_waiters[key] = event

        try:
            size = len(content)
            headers = dict(_UPLOAD_HEADERS)
            headers.update({
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(size),
                "Content-Range": f"0-{max(size - 1, 0)}/{size}",
            })
            requests.post(url, headers=headers, data=content, timeout=120)

            if not event.wait(wait_timeout):
                # сервер не подтвердил обработку — но fileId обычно уже валиден,
                # поэтому не падаем, а лишь предупреждаем
                print(f"[max] предупреждение: нет push 136 для fileId {file_id}")
        finally:
            with self._upload_waiters_lock:
                self._upload_waiters.pop(key, None)

        return {"_type": "FILE", "fileId": file_id}

    # region upload_video()
    def upload_video(self, content: bytes, filename: str = "video.mp4",
                     wait_timeout: float = 120.0) -> dict:
        """
        Загружает видео в MAX и возвращает attach-элемент.

        Процесс: opcode 82 (запрос upload-URL) -> сырой POST с Content-Range ->
        ожидание серверного push opcode 136 (видео обработано) -> attach.

        Returns:
            dict вида {"_type": "VIDEO", "videoId": <int>, "token": "<token>"}.

        Raises:
            RuntimeError: если сервер не вернул URL/videoId/token.
        """
        recv = self._send_and_wait(82, {"count": 1})
        info_list = recv["payload"].get("info") or []
        if not info_list:
            raise RuntimeError(f"MAX не вернул info для загрузки видео: {recv['payload']}")
        info = info_list[0]
        url = info.get("url")
        video_id = info.get("videoId")
        token = info.get("token")
        if not url or video_id is None or not token:
            raise RuntimeError(f"Неполный ответ загрузки видео: {info}")

        # регистрируем ожидание готовности ДО POST, чтобы не пропустить push 136
        key = f"video:{video_id}"
        event = threading.Event()
        with self._upload_waiters_lock:
            self._upload_waiters[key] = event

        try:
            size = len(content)
            headers = dict(_UPLOAD_HEADERS)
            headers.update({
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(size),
                "Content-Range": f"0-{max(size - 1, 0)}/{size}",
            })
            requests.post(url, headers=headers, data=content, timeout=300)

            if not event.wait(wait_timeout):
                # видео могло не успеть обработаться на сервере — предупреждаем,
                # но всё равно пробуем отправить (videoId уже валиден)
                print(f"[max] предупреждение: нет push 136 для videoId {video_id}")
        finally:
            with self._upload_waiters_lock:
                self._upload_waiters.pop(key, None)

        return {"_type": "VIDEO", "videoId": video_id, "token": token}

    # region get_video_url()
    def get_video_url(self, video_id, token: str = None, diag: bool = False) -> str | None:
        """
        Получает воспроизводимый URL видео по videoId (opcode 83 VIDEO_PLAY).

        В attach входящего видео нет прямой ссылки — только videoId/token, поэтому
        для пересылки в Telegram нужно запросить URL отдельно.

        Returns:
            URL видео (предпочтительно mp4) или None, если не удалось.
        """
        payload = {"videoId": video_id}
        if token:
            payload["token"] = token
        try:
            recv = self._send_and_wait(83, payload)
        except Exception as e:
            print("[max] get_video_url ошибка:", e)
            return None

        p = recv.get("payload", {})
        if diag:
            print("[diag] video_play payload keys:", list(p.keys()), "->", json.dumps(p, ensure_ascii=False)[:600])

        # Структура ответа MAX может содержать набор качеств. Берём лучший
        # доступный mp4-URL. Поддерживаем несколько вероятных схем именования.
        for key in ("VIDEO_HD", "VIDEO_SD", "VIDEO_LOW", "VIDEO_MOBILE", "url", "URL"):
            val = p.get(key)
            if isinstance(val, str) and val.startswith("http"):
                return val
        # вариант со списком/словарём качеств
        videos = p.get("videos") or p.get("urls")
        if isinstance(videos, dict):
            for v in videos.values():
                if isinstance(v, str) and v.startswith("http"):
                    return v
        if isinstance(videos, list):
            for v in videos:
                if isinstance(v, str) and v.startswith("http"):
                    return v
                if isinstance(v, dict):
                    u = v.get("url") or v.get("URL")
                    if isinstance(u, str) and u.startswith("http"):
                        return u
        return None

    # region send_message()
    def send_message(self, chat_id: int, text: str, reply_id: str|int = None, notify: bool = True, on_cid=None, attaches: list = None):
        """
        Sends a text message to a specified chat.

        This method constructs and sends a text message to the given chat ID, with an optional reply to another message.
        It waits for the server response with the matching sequence number and returns a `Message` object.

        Args:
            chat_id (int): The ID of the chat to send the message to.
            text (str): The text content of the message.
            reply_id (str | int, optional): The ID of the message to reply to. Defaults to None.
            notify (bool, optional): Whether to notify chat participants. Defaults to True.

        Returns:
            Message: A `Message` object representing the sent message.

        Raises:
            ValueError: If the client is not connected or authenticated.
            Exception: If the server response cannot be parsed into a `Message` object.

        Usage:
            ```python
            # Send a simple message
            msg = client.send_message(12345678, "Hello, world!")
            
            # Send a message with a reply
            msg = client.send_message(12345678, "Replying to you!", reply_id=987654)
            ```
        """
        cid = self.cid
        if on_cid is not None:
            # даём вызывающему запомнить cid ДО отправки, чтобы гарантированно
            # перехватить эхо этого сообщения (opcode 128) и не зациклить мост
            on_cid(cid)

        message = {
            "text": text,
            "cid": cid,
            "elements": [],
            "attaches": attaches or []
        }
        if reply_id:
            message["link"] = {
                "type": "REPLY",
                "messageId": str(reply_id)
            }

        recv = self._send_and_wait(64, {
            "chatId": chat_id,
            "message": message,
            "notify": notify
        })
        payload = recv["payload"]
        msg = Message(self, payload["chatId"], **payload["message"], _f=1)
        return msg

    # region delete_message()
    def delete_message(self, chat_id: int, message_ids: list[str], for_me: bool = False):
        """
        Deletes one or more messages from a specified chat.

        This method sends a request to delete messages identified by their IDs in the given chat.
        The `for_me` parameter determines whether the deletion is only for the current user or for all chat participants.

        Args:
            chat_id (int): The ID of the chat containing the messages.
            message_ids (list[str]): A list of message IDs to delete.
            for_me (bool, optional): If True, deletes the messages only for the current user. Defaults to False.

        Raises:
            ValueError: If the client is not connected or authenticated.

        Usage:
            ```python
            # Delete messages for all participants
            client.delete_message(12345678, ["1000121", "1000122"])
            
            # Delete messages only for the current user
            client.delete_message(12345678, ["1000120"], for_me=True)
            ```
        """
        self._raw_send(json.dumps({
            "ver":11,
            "cmd":0,
            "seq":self.seq,
            "opcode":66,
            "payload": {
                "chatId":chat_id,
                "messageIds": message_ids,
                "forMe": for_me
            }
        }))

    # region edit_message()
    def edit_message(self, chat_id: int, message_id: str|int, text: str):
        """
        Edits the text of an existing message in a specified chat.

        This method sends a request to update the text of a message identified by its ID in the given chat.
        It waits for the server response with the matching sequence number and returns the updated `Message` object.

        Args:
            chat_id (int): The ID of the chat containing the message.
            message_id (str | int): The ID of the message to edit.
            text (str): The new text content for the message.

        Returns:
            Message: A `Message` object representing the edited message.

        Raises:
            ValueError: If the client is not connected, not authenticated, or the response cannot be parsed.

        Usage:
            ```python
            # Edit an existing message
            updated_msg = client.edit_message(12345678, "12111121", "New text")
            ```
        """
        recv = self._send_and_wait(67, {
            "chatId": chat_id,
            "messageId": str(message_id),
            "text": text,
            "elements": [],
            "attachments": []
        })
        payload = recv["payload"]
        msg = Message(self, chat_id, **payload["message"])
        return msg
    
    # region pin_chat()
    def pin_chat(self, chat_id: int|str):
        j = {
            "ver": 11,
            "cmd": 0,
            "seq": self.seq,
            "opcode": 22,
            "payload": {
                "settings": {
                    "chats": {
                        str(chat_id): {
                            "favIndex": int(time.time()*1000)
                        }
                    }
                }
            }
        }
        self._raw_send(json.dumps(j))
        return True

    # region unpin_chat()
    def unpin_chat(self, chat_id: int|str):
        j = {
            "ver": 11,
            "cmd": 0,
            "seq": self.seq,
            "opcode": 22,
            "payload": {
                "settings": {
                    "chats": {
                        str(chat_id): {
                            "favIndex": 0
                        }
                    }
                }
            }
        }
        self._raw_send(json.dumps(j))
        return True

    # region get_user()
    def get_user(self, **kwargs):
        """
        Retrieves a user's profile by their ID or phone number.

        Args:
            - id (int, optional) : The contact ID of the user to retrieve.
            - phone (str, optional) : The phone number of the user to retrieve.
            - chat_id (int, optional) : The chat ID with the user to retrieve.

        Returns:
            User: A `User` object representing the retrieved user's profile.

        Raises:
            ValueError: If neither `id` nor `phone` is provided, or if the client is not connected or authenticated.
            WebSocketError: If there is an issue with the WebSocket communication.

        Usage:
            ```python
            # Get user by ID
            user = client.get_user(id="123456")
            print(user.contact.names[0].name)  # Prints the user's full name

            # Get user by phone number
            user = client.get_user(phone="+7xxxxxxxxxx")
            print(user.contact.phone)  # Prints the user's phone number
        """
        id = kwargs.get('id')
        phone = kwargs.get('phone')
        chat_id = kwargs.get('chat_id')
        _f = kwargs.get("_f")

        if id:
            opcode, req_payload = 32, {"contactIds": [id]}
        elif phone:
            opcode, req_payload = 46, {"phone": str(phone)}
        elif chat_id:
            id = self.me.contact.id ^ chat_id
            opcode, req_payload = 32, {"contactIds": [id]}
        else:
            raise ValueError("no `id` or `phone` or `chat_id` provided")

        recv = self._send_and_wait(opcode, req_payload)

        payload = recv["payload"]

        error = payload.get("error")

        if error:
            raise UserNotFound(error, payload["message"]+f": {phone}")

        if id:
            contact = payload["contacts"][0]
        if phone:
            payload["contact"]["phone"] = phone
            contact = payload["contact"]

        return User(self, contact, _f)

    # region _cache_sync()
    def _cache_sync(self, payload: dict):
        """Складывает чаты и контакты из ответа логина (opcode 19) в кэш."""
        for raw_chat in payload.get("chats", []):
            cid = raw_chat.get("id")
            if cid is not None:
                self.chats[cid] = raw_chat
        for raw_contact in payload.get("contacts", []):
            cid = raw_contact.get("id")
            if cid is not None:
                self.contacts[cid] = raw_contact

    # region _contact_name()
    def _contact_name(self, contact_id: int) -> str | None:
        """Возвращает отображаемое имя контакта по id (из кэша или запросом opcode 32)."""
        raw = self.contacts.get(contact_id)
        if raw is None:
            try:
                user = self.get_user(id=contact_id, _f=1)
                names = user.contact.names
                return names[0].name if names else None
            except Exception:
                return None
        names = raw.get("names") or []
        if names:
            return names[0].get("name")
        return None

    # region get_chat_title()
    def get_chat_title(self, chat_id: int) -> str:
        """
        Возвращает человекочитаемое название чата для именования топика.

        - CHAT / CHANNEL: поле `title`.
        - DIALOG: имя собеседника (берётся из контакта, у диалога title нет).
        - Фолбэк: «Чат <id>».
        """
        if chat_id in self._title_cache:
            return self._title_cache[chat_id]

        title = None
        raw = self.chats.get(chat_id)
        if raw is not None:
            chat_type = raw.get("type")
            if chat_type in ("CHAT", "CHANNEL"):
                title = raw.get("title")
            elif chat_type == "DIALOG":
                # собеседник = участник диалога, отличный от меня
                me_id = self.me.contact.id if self.me else None
                participants = raw.get("participants") or {}
                other_id = None
                for pid in participants.keys():
                    try:
                        pid_int = int(pid)
                    except (TypeError, ValueError):
                        continue
                    if pid_int != me_id:
                        other_id = pid_int
                        break
                # запасной путь: chatId диалога = me_id XOR other_id
                if other_id is None and me_id is not None:
                    other_id = me_id ^ chat_id
                if other_id is not None:
                    title = self._contact_name(other_id)

        if not title:
            # на случай личного диалога, которого нет в кэше чатов
            me_id = self.me.contact.id if self.me else None
            if me_id is not None:
                title = self._contact_name(me_id ^ chat_id)

        if not title:
            title = f"Чат {chat_id}"

        self._title_cache[chat_id] = title
        return title

    # region session_exit()
    def session_exit(self):
        """Terminates active session token. **There no way back.**"""
        j = {"ver":11,"cmd":0,"seq":self.seq,"opcode":20,"payload":{}}
        self._raw_send(json.dumps(j))
        self.disconnect()
        return True
    
    # region set_reaction()
    def set_reaction(self, chat_id, message_id, reaction: EMOJIS):
        """
        Sets a reaction to a specific message in a chat.

        This function sends a reaction to a message and return a Reactions object.

        Args:
            chat_id (str): The unique identifier of the chat.
            message_id (str): The unique identifier of the message.
            reaction (EMOJIS): The emoji reaction to be set, using the EMOJIS enumeration.

        Returns:
            Reactions: An object containing information about the updated message reactions,
                    including counters for each emoji, your own reaction, and the total count.
        """
        recv = self._send_and_wait(178, {
            "chatId": chat_id,
            "messageId": message_id,
            "reaction": {"reactionType": "EMOJI", "id": reaction}
        })
        payload = recv["payload"]
        return Reactions(**payload)
    
    # region contact_add()
    def contact_add(self, user_id: int):
        recv = self._send_and_wait(34, {"contactId": user_id, "action": "ADD"})
        payload = recv["payload"]
        return User(self, payload["contact"])

    # region contact_remove()
    def contact_remove(self, user_id: int):
        self._send_and_wait(34, {"contactId": user_id, "action": "REMOVE"})
        return True

    # region contact_block()
    def contact_block(self, user_id: int):
        self._send_and_wait(34, {"contactId": user_id, "action": "BLOCK"})
        return True

    # region contact_unblock()
    def contact_unblock(self, user_id: int):
        self._send_and_wait(34, {"contactId": user_id, "action": "UNBLOCK"})
        return True
                
    # region @on_message()
    def on_message(self, filters):
        """
        Decorator to register a handler for a specific message type.

        This allows defining functions to handle certain events or messages by text key.

        Usage:
        ```
        from max import MaxClient as Client
        from filters import filters
        from classes import Message

        client = Client("token")

        @client.on_message(filters.command("hello"))
        def command_hello(client: Client, message: Message):
            message.reply("Max - самый забагованный мессенджер.")
        # The decorated function will be called when the filter event occurs.

        client.run()
        ```
        """
        def decorator(func):
            self.handlers.append((filters, func))
            return func

        return decorator
    
    #region @on_connect
    def on_connect(self, func):
        """
        Registers a callback function to be called upon successful connection.

        This sets a handler that is invoked after connecting and authenticating.

        Usage:
        ```
        from max import MaxClient as Client

        client = Client("token")

        @client.on_connect
        def on_connect_handler():
            print("Connected!")
        # The function will be called automatically on connect.
        client.run()
        ```
        """
        self._on_connect = func
        return func
