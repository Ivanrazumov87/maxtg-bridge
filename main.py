from max import MaxClient as Client
from filters import filters
from classes import Message
from bridge import Bridge
import os
from dotenv import load_dotenv

load_dotenv()

MAX_TOKEN = os.getenv("MAX_TOKEN")

# Опциональный whitelist чатов MAX. Пусто -> пересылаются ВСЕ чаты.
_raw_chats = (os.getenv("MAX_CHAT_IDS") or "").strip()
MAX_CHAT_IDS = [int(x) for x in _raw_chats.split(",") if x.strip()] if _raw_chats else []

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
# ID форум-супергруппы Telegram (с включёнными топиками). Бот должен быть в ней
# администратором с правом управления топиками.
TG_GROUP_ID = os.getenv("TG_GROUP_ID")

if not MAX_TOKEN or not TG_BOT_TOKEN or not TG_GROUP_ID:
    print("Ошибка в .env: проверьте MAX_TOKEN, TG_BOT_TOKEN, TG_GROUP_ID")

client = Client(MAX_TOKEN)
bridge = Bridge(client, TG_BOT_TOKEN, TG_GROUP_ID)


@client.on_connect
def onconnect():
    if client.me is not None:
        print(f"Имя: {client.me.contact.names[0].name}, Номер: {client.me.contact.phone} | ID: {client.me.contact.id}")
    # запускаем приём из Telegram (TG -> MAX) после успешного логина
    bridge.start_telegram_poller()


@client.on_message(filters.any())
def onmessage(client: Client, message: Message):
    # whitelist (если задан) + игнор удалённых
    if MAX_CHAT_IDS and message.chat.id not in MAX_CHAT_IDS:
        return
    if message.status == "REMOVED":
        return

    # служебное сообщение без отправителя (уведомление о действии в чате и т.п.)
    if message.sender is None or message.user is None:
        return

    msg_text = message.text
    msg_attaches = message.attaches
    try:
        name = message.user.contact.names[0].name
    except (IndexError, AttributeError):
        name = "Без имени"

    if "link" in message.kwargs.keys():
        if "type" in message.kwargs["link"]:
            if message.kwargs["link"]["type"] == "REPLY":  # TODO
                ...
            if message.kwargs["link"]["type"] == "FORWARD":
                msg_text = message.kwargs["link"]["message"]["text"]
                msg_attaches = message.kwargs["link"]["message"]["attaches"]
                forwarded_msg_author = client.get_user(id=message.kwargs["link"]["message"]["sender"], _f=1)
                name = f"{name}\n(Переслано: {forwarded_msg_author.contact.names[0].name})"

    # моё ли это исходящее сообщение (написал я сам) — такие шлём в Telegram тихо
    is_own = client.me is not None and message.sender == client.me.contact.id

    if msg_text != "" or msg_attaches != []:
        bridge.on_max_message(
            name=name,
            text=msg_text,
            attaches=msg_attaches,
            max_chat_id=message.chat.id,
            cid=message.cid,
            silent=is_own,
        )


import threading

client.run()

threading.Event().wait()
