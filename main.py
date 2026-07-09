from max import MaxClient as Client
from filters import filters
from classes import Message
from bridge import Bridge
import os
from dotenv import load_dotenv

load_dotenv()

MAX_TOKEN = os.getenv("MAX_TOKEN")

# ID группы MAX, которую зеркалим в Telegram (жёсткая привязка одна-к-одной).
_raw_max_group = (os.getenv("MAX_GROUP_ID") or "").strip()
MAX_GROUP_ID = int(_raw_max_group) if _raw_max_group else None

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
# ID обычной группы Telegram, привязанной к группе MAX. Бот должен быть в ней
# участником (и админом с отключённым privacy mode, чтобы видеть все сообщения).
TG_GROUP_ID = os.getenv("TG_GROUP_ID")

if not MAX_TOKEN or not TG_BOT_TOKEN or not TG_GROUP_ID or not MAX_GROUP_ID:
    print("Ошибка в .env: проверьте MAX_TOKEN, TG_BOT_TOKEN, TG_GROUP_ID, MAX_GROUP_ID")

client = Client(MAX_TOKEN)
bridge = Bridge(client, TG_BOT_TOKEN, TG_GROUP_ID, MAX_GROUP_ID)


@client.on_connect
def onconnect():
    if client.me is not None:
        print(f"Имя: {client.me.contact.names[0].name}, Номер: {client.me.contact.phone} | ID: {client.me.contact.id}")
    # запускаем приём из Telegram (TG -> MAX) после успешного логина
    bridge.start_telegram_poller()


@client.on_message(filters.any())
def onmessage(client: Client, message: Message):
    # интересует только привязанная группа MAX
    if MAX_GROUP_ID is not None and message.chat.id != MAX_GROUP_ID:
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

    if msg_text != "" or msg_attaches != []:
        bridge.on_max_message(
            name=name,
            text=msg_text,
            attaches=msg_attaches,
            max_chat_id=message.chat.id,
            cid=message.cid,
        )


import threading

client.run()

threading.Event().wait()
