# -*- coding: utf-8 -*-
"""Двоичный протокол веб-сокета MAX (с 18.11.2026).

Что случилось: 18.11.2026 около 14:20 МСК MAX выключил старый адрес
wss://ws-api.oneme.ru/websocket (TCP-соединение не устанавливается ни с VPS,
ни с ПК пользователя). Веб-версия MAX (web.max.ru) теперь ходит на
wss://api.oneme.ru/websocket, и там другой формат кадра: не текст JSON,
а двоичный кадр. Устройство кадра снято с кода веб-клиента
(web.max.ru, _app/immutable/chunks/RE-seo4Q.js, функции кодирования
и разбора кадра, версия приложения 26.11.9):

    байт 0      версия протокола = 10
    байт 1      cmd   (0 - запрос/пуш, 1 - ответ, 2 - повтор, 3 - ошибка)
    байты 2-3   seq   (int16, big-endian)
    байты 4-5   opcode (int16, big-endian)
    байт 6      сжатие: 0 - нет; N>0 - тело сжато LZ4 (блочный формат),
                распакованное не длиннее len(тело) * N
    байты 7-9   длина тела (24 бита, big-endian)
    дальше      тело - msgpack; большие целые веб-клиент шлёт расширением
                типа 1 (внутри - msgpack-целое)

Содержимое сообщений (поля payload) осталось прежним - меняется только
«конверт». Поэтому остальной код клиента работает со словарями
{"cmd", "seq", "opcode", "payload"}, как и раньше с JSON.
"""
import json
import struct

import msgpack

WS_URL = "wss://api.oneme.ru/websocket"
ORIGIN = "https://web.max.ru"
APP_VERSION = "26.11.9"
PROTO_VERSION = 10

_HEADER = struct.Struct(">BBhhB")   # версия, cmd, seq, opcode, сжатие
HEADER_LEN = 10                     # + 3 байта длины
MAX_BODY = (1 << 24) - 1
SEQ_MOD = 0x8000                    # seq - знаковое int16, держим в 0..32767


class FrameError(ValueError):
    """Кадр MAX не удалось разобрать."""


def lz4_block_decompress(src, max_out=None):
    """Распаковка блока LZ4 (без заголовка кадра LZ4 - так шлёт MAX)."""
    dst = bytearray()
    i, n = 0, len(src)
    try:
        while i < n:
            token = src[i]
            i += 1
            lit = token >> 4
            if lit == 15:
                while True:
                    b = src[i]
                    i += 1
                    lit += b
                    if b != 255:
                        break
            if i + lit > n:
                raise FrameError("LZ4: литералы выходят за конец блока")
            # предел проверяем ДО копирования (замечание Codex: иначе битый
            # кадр мог бы занять гигабайты памяти раньше проверки)
            if max_out is not None and len(dst) + lit > max_out:
                raise FrameError("LZ4: распакованное длиннее заявленного")
            dst += src[i:i + lit]
            i += lit
            if i >= n:
                break                       # последний блок - только литералы
            off = src[i] | (src[i + 1] << 8)
            i += 2
            if off == 0 or off > len(dst):
                raise FrameError(f"LZ4: неверное смещение {off}")
            ml = (token & 15) + 4
            if (token & 15) == 15:
                while True:
                    b = src[i]
                    i += 1
                    ml += b
                    if b != 255:
                        break
            if max_out is not None and len(dst) + ml > max_out:
                raise FrameError("LZ4: распакованное длиннее заявленного")
            start = len(dst) - off
            if off >= ml:
                dst += dst[start:start + ml]
            else:                           # перекрытие - копируем побайтно
                for k in range(ml):
                    dst.append(dst[start + k])
    except IndexError:
        raise FrameError("LZ4: блок оборван") from None
    return bytes(dst)


def _ext_hook(code, data):
    if code == 1:                           # большое целое (BigInt веб-клиента)
        return msgpack.unpackb(data)
    return msgpack.ExtType(code, data)


JS_SAFE_INT = 2 ** 53 - 1


def _wrap_bigints(obj):
    """Целые вне ±(2^53-1) - расширением типа 1, как их шлёт веб-клиент
    (в JavaScript это BigInt). Меньшие остаются обычными целыми msgpack."""
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        if -JS_SAFE_INT <= obj <= JS_SAFE_INT:
            return obj
        return msgpack.ExtType(1, msgpack.packb(obj))
    if isinstance(obj, dict):
        return {k: _wrap_bigints(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_wrap_bigints(v) for v in obj]
    return obj


def encode_frame(obj):
    """Словарь {"cmd", "seq", "opcode", "payload"} -> двоичный кадр.
    Принимает и строку JSON (так кадры собирал старый код клиента)."""
    if isinstance(obj, (str, bytes)):
        obj = json.loads(obj)
    payload = obj.get("payload")
    body = (b"" if payload is None
            else msgpack.packb(_wrap_bigints(payload), use_bin_type=True))
    if len(body) > MAX_BODY:
        raise FrameError(f"тело кадра {len(body)} байт - больше 16 МБ")
    n = len(body)
    seq = int(obj.get("seq", 0)) % SEQ_MOD
    head = _HEADER.pack(PROTO_VERSION, int(obj.get("cmd", 0)), seq,
                        int(obj["opcode"]), 0)
    return head + bytes(((n >> 16) & 255, (n >> 8) & 255, n & 255)) + body


def decode_frame(data):
    """Двоичный кадр -> словарь {"ver", "cmd", "seq", "opcode", "payload"}."""
    if isinstance(data, str):               # текстовый кадр старого протокола
        return json.loads(data)
    if len(data) < HEADER_LEN:
        raise FrameError(f"кадр короче заголовка: {len(data)} байт")
    ver, cmd, seq, opcode, comp = _HEADER.unpack_from(data, 0)
    n = (data[7] << 16) | (data[8] << 8) | data[9]
    body = data[HEADER_LEN:HEADER_LEN + n]
    if len(body) != n:
        raise FrameError(f"длина тела {len(body)} вместо {n}")
    payload = None
    if n:
        if comp:
            body = lz4_block_decompress(body, n * comp)
        try:
            payload = msgpack.unpackb(body, ext_hook=_ext_hook, raw=False,
                                      strict_map_key=False)
        except Exception as e:
            raise FrameError(f"msgpack: {type(e).__name__} {e}") from None
    return {"ver": ver, "cmd": cmd, "seq": seq, "opcode": opcode,
            "payload": payload}
