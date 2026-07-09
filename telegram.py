import requests, json

API = "https://api.telegram.org/bot{token}/{method}"


def _call(token: str, method: str, **kwargs) -> dict:
    """Вызов метода Telegram Bot API. Возвращает разобранный JSON-ответ."""
    url = API.format(token=token, method=method)
    # дефолтный таймаут, чтобы зависший HTTP не держал воркер бесконечно
    kwargs.setdefault("timeout", 30)
    resp = requests.post(url, **kwargs)
    return resp.json()


# region download_file
def download_file(token: str, file_id: str) -> bytes | None:
    """
    Скачивает файл Telegram по file_id: getFile -> file_path -> загрузка байтов.

    Returns:
        bytes содержимого файла или None при ошибке.
    """
    resp = _call(token, "getFile", data={"file_id": file_id})
    if not resp.get("ok"):
        print("getFile error:", resp)
        return None
    file_path = resp["result"].get("file_path")
    if not file_path:
        return None
    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    r = requests.get(url, timeout=120)
    if r.status_code != 200:
        print("file download error:", r.status_code)
        return None
    return r.content


def handle_attach(attach: dict) -> str:
    # .get() вместо жёсткого subscript: у нестандартного/недообработанного
    # attach может не быть ожидаемого ключа — не роняем обработку сообщения
    match attach.get("_type"):
        case "FILE":
            return attach.get("name") or "файл"
        case _:
            return attach.get("_type") or "вложение"


# region send_video
def send_video(token: str, chat_id: int | str, video_url: str, caption: str = "",
               message_thread_id: int | None = None,
               disable_notification: bool = False) -> dict:
    """
    Отправляет видео в Telegram по URL (Telegram сам скачает его с video_url).

    Если Telegram не сможет принять по URL (большой/недоступный файл), вернёт
    ok=false — вызывающий код должен предусмотреть фолбэк.
    """
    data = {
        "chat_id": chat_id,
        "video": video_url,
        "supports_streaming": True,
    }
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    if message_thread_id is not None:
        data["message_thread_id"] = message_thread_id
    if disable_notification:
        data["disable_notification"] = True
    return _call(token, "sendVideo", data=data)


# Лимит на видео, отправляемое multipart-ом через Bot API (~50 МБ). Крупнее
# Telegram не принимает — такое видео пересылаем ссылкой/пометкой.
TG_UPLOAD_LIMIT = 50 * 1024 * 1024

# Браузерный UA — CDN OK (okcdn.ru) без него может отдать заглушку.
_DL_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
}


# region download_url_bytes
def download_url_bytes(url: str, limit: int = TG_UPLOAD_LIMIT) -> bytes | None:
    """
    Скачивает содержимое по URL (для видео из MAX, когда Telegram сам скачать
    по ссылке не смог). Возвращает bytes или None (ошибка / превышен лимит).
    Читает потоково и обрывается, если размер превышает limit.
    """
    try:
        with requests.get(url, headers=_DL_HEADERS, stream=True, timeout=120) as r:
            if r.status_code != 200:
                print("download_url_bytes: HTTP", r.status_code)
                return None
            chunks = []
            total = 0
            for chunk in r.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                total += len(chunk)
                if total > limit:
                    print("download_url_bytes: файл больше лимита", limit)
                    return None
                chunks.append(chunk)
            return b"".join(chunks)
    except Exception as e:
        print("download_url_bytes ошибка:", e)
        return None


# region send_video_bytes
def send_video_bytes(token: str, chat_id: int | str, content: bytes,
                     caption: str = "", filename: str = "video.mp4",
                     message_thread_id: int | None = None,
                     disable_notification: bool = False) -> dict:
    """
    Отправляет видео в Telegram, заливая сами байты (multipart). Работает даже
    когда sendVideo по URL не сработал (подписанный/host-restricted CDN-URL).
    """
    data = {"chat_id": chat_id, "supports_streaming": True}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    if message_thread_id is not None:
        data["message_thread_id"] = message_thread_id
    if disable_notification:
        data["disable_notification"] = True
    files = {"video": (filename, content, "video/mp4")}
    return _call(token, "sendVideo", data=data, files=files)


# region createForumTopic
def create_forum_topic(token: str, chat_id: int | str, name: str) -> int | None:
    """
    Создаёт топик в форум-супергруппе и возвращает его message_thread_id.

    Returns:
        int message_thread_id при успехе, иначе None (с печатью ошибки).
    """
    # Telegram ограничивает имя топика 128 символами
    name = (name or "Чат")[:128]
    resp = _call(token, "createForumTopic", data={"chat_id": chat_id, "name": name})
    if resp.get("ok"):
        return resp["result"]["message_thread_id"]
    print("createForumTopic error:", resp)
    return None


# region getUpdates
def get_updates(token: str, offset: int = 0, timeout: int = 30) -> list[dict]:
    """
    Long-polling приём апдейтов. Возвращает список updates.

    Слушаем только сообщения (message), этого достаточно для моста.
    """
    resp = _call(
        token,
        "getUpdates",
        data={
            "offset": offset,
            "timeout": timeout,
            "allowed_updates": json.dumps(["message"]),
        },
        timeout=timeout + 10,
    )
    if resp.get("ok"):
        return resp.get("result", [])
    print("getUpdates error:", resp)
    return []


# region send_to_telegram
def send_to_telegram(
    TG_BOT_TOKEN: str = "",
    TG_CHAT_ID: int = 0,
    caption: str = "",
    attachments: list[dict] = [],
    message_thread_id: int | None = None,
    disable_notification: bool = False,
) -> dict | None:
    """
    Отправляет сообщение (текст и/или вложения) в чат Telegram.

    Если задан message_thread_id — сообщение уходит в соответствующий топик
    форум-супергруппы. disable_notification=True — отправить «тихо» (без звука
    и всплывающего уведомления); используется для собственных исходящих
    сообщений пользователя.
    """
    if not attachments:  # нет вложений — просто текст
        if caption == "":
            return None
        data = {
            "chat_id": TG_CHAT_ID,
            "text": caption,
            "parse_mode": "HTML",
        }
        if message_thread_id is not None:
            data["message_thread_id"] = message_thread_id
        if disable_notification:
            data["disable_notification"] = True
        resp = _call(TG_BOT_TOKEN, "sendMessage", data=data)
        return resp

    if 1 <= len(attachments) <= 10:
        media = []
        not_handled_attachs = attachments.copy()
        for i, attach in enumerate(attachments):
            if attach.get("_type") == "PHOTO":
                # URL картинки может отсутствовать (недообработанное фото,
                # линк-превью и т.п.) — тогда оставляем attach в необработанных,
                # а не роняем всё сообщение жёстким subscript.
                url = attach.get("baseUrl")
                if not url:
                    continue
                item = {"type": "photo", "media": url}
                not_handled_attachs.remove(attach)
                if i == 0 and caption:
                    item["caption"] = caption
                    item["parse_mode"] = "HTML"
                media.append(item)
        if not_handled_attachs:
            if media:
                print(not_handled_attachs)
                media[0].setdefault("caption", caption)
                media[0]["caption"] += "\n\nНеобработанные файлы: " + ", ".join(
                    handle_attach(attach) for attach in not_handled_attachs
                )
                media[0]["parse_mode"] = "HTML"
            else:
                send_to_telegram(
                    TG_BOT_TOKEN,
                    TG_CHAT_ID,
                    caption + "\n\nНеобработанные файлы: " + ", ".join(
                        handle_attach(attach) for attach in not_handled_attachs
                    ),
                    message_thread_id=message_thread_id,
                    disable_notification=disable_notification,
                )
                return None

        # если ни одно фото не дало пригодного URL — media пуст; sendMediaGroup
        # пустой массив не примет, поэтому шлём хотя бы текст/пометку
        if not media:
            return None

        data = {
            "chat_id": TG_CHAT_ID,
            "media": json.dumps(media),
        }
        if message_thread_id is not None:
            data["message_thread_id"] = message_thread_id
        if disable_notification:
            data["disable_notification"] = True
        resp = _call(TG_BOT_TOKEN, "sendMediaGroup", data=data)
        # фото Telegram качает с URL MAX сам; если URL под auth/referer/токеном,
        # ответ будет ok:false — логируем причину, иначе потеря совершенно немая
        if resp and not resp.get("ok"):
            print("sendMediaGroup error:", resp.get("description"), "| media:", media)
        return resp

    # если вложений больше 10 — разобьём на несколько альбомов
    for i in range(0, len(attachments), 10):
        chunk = attachments[i:i + 10]
        send_to_telegram(
            TG_BOT_TOKEN,
            TG_CHAT_ID,
            caption if i == 0 else "",
            chunk,
            message_thread_id=message_thread_id,
            disable_notification=disable_notification,
        )
    return None
