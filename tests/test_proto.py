# -*- coding: utf-8 -*-
"""Проверка двоичного протокола MAX (proto.py) без сети.

Запуск: python -m unittest discover -s tests
Сверка LZ4 с эталонной библиотекой lz4 - только если она установлена
(для работы моста она не нужна: распаковка своя).
"""
import os
import random
import struct
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import msgpack
from proto import (encode_frame, decode_frame, lz4_block_decompress, FrameError,
                   SEQ_MOD)

try:
    import lz4.block as lz4block
except ImportError:
    lz4block = None


def frame(cmd, seq, opcode, body, comp=0):
    n = len(body)
    return (struct.pack(">BBhhB", 10, cmd, seq, opcode, comp)
            + bytes(((n >> 16) & 255, (n >> 8) & 255, n & 255)) + body)


class FrameTest(unittest.TestCase):
    def test_header_layout(self):
        # заголовок как у веб-клиента MAX: версия 10, cmd, seq, opcode,
        # сжатие 0, длина 24 бита; тело - msgpack
        data = encode_frame({"cmd": 0, "seq": 5, "opcode": 19, "payload": {"a": 1}})
        body = msgpack.packb({"a": 1})
        self.assertEqual(data[:10], bytes([10, 0, 0, 5, 0, 19, 0, 0, 0, len(body)]))
        self.assertEqual(data[10:], body)

    def test_roundtrip_and_json_string_input(self):
        payload = {"chatId": -68123456789012, "message": {"text": "Поступление 8 441,27 ₽",
                   "attaches": [{"_type": "PHOTO", "photoToken": "x" * 300}]},
                   "notify": True}
        for src in ({"cmd": 0, "seq": 7, "opcode": 64, "payload": payload},
                    '{"ver": 11, "cmd": 0, "seq": 7, "opcode": 64, "payload": %s}'
                    % __import__("json").dumps(payload)):
            got = decode_frame(encode_frame(src))
            self.assertEqual((got["cmd"], got["seq"], got["opcode"]), (0, 7, 64))
            self.assertEqual(got["payload"], payload)

    def test_empty_payload(self):
        data = encode_frame({"cmd": 1, "seq": 3, "opcode": 1, "payload": None})
        self.assertEqual(len(data), 10)
        self.assertIsNone(decode_frame(data)["payload"])

    def test_seq_wraps_into_int16(self):
        got = decode_frame(encode_frame({"cmd": 0, "seq": SEQ_MOD + 4, "opcode": 1}))
        self.assertEqual(got["seq"], 4)

    def test_bigint_extension(self):
        # большое целое веб-клиент шлёт расширением типа 1
        big = 2 ** 60 + 12345
        body = msgpack.packb({"id": msgpack.ExtType(1, msgpack.packb(big))})
        got = decode_frame(frame(1, 0, 19, body))
        self.assertEqual(got["payload"]["id"], big)

    def test_bigint_outbound(self):
        # наружу большие целые - тоже расширением 1, малые - обычными
        big = -(2 ** 60)
        data = encode_frame({"cmd": 0, "seq": 1, "opcode": 64,
                             "payload": {"chatId": big, "n": [7, 2 ** 53], "ok": True}})
        raw = msgpack.unpackb(data[10:], strict_map_key=False)
        self.assertEqual(raw["chatId"], msgpack.ExtType(1, msgpack.packb(big)))
        self.assertEqual(raw["n"][0], 7)
        self.assertIsInstance(raw["n"][1], msgpack.ExtType)
        self.assertIs(raw["ok"], True)
        got = decode_frame(data)["payload"]
        self.assertEqual((got["chatId"], got["n"]), (big, [7, 2 ** 53]))

    def test_int_map_keys(self):
        body = msgpack.packb({123: "a"})
        self.assertEqual(decode_frame(frame(1, 0, 19, body))["payload"], {123: "a"})

    def test_text_frame_still_json(self):
        self.assertEqual(decode_frame('{"opcode": 6, "payload": {}}')["opcode"], 6)

    def test_broken_frames(self):
        with self.assertRaises(FrameError):
            decode_frame(b"\x0a\x01")
        good = frame(1, 0, 19, msgpack.packb({"a": 1}))
        with self.assertRaises(FrameError):
            decode_frame(good[:-1])          # тело оборвано
        with self.assertRaises(FrameError):
            decode_frame(frame(1, 0, 19, b"\xc1"))   # 0xc1 - не msgpack


class Lz4Test(unittest.TestCase):
    def test_handmade_overlap_match(self):
        # 'a' и совпадение длиной 9 со смещением 1 -> 10 букв 'a'
        self.assertEqual(lz4_block_decompress(bytes([0x15, 0x61, 0x01, 0x00])), b"a" * 10)

    def test_handmade_long_literal_and_match(self):
        lit = bytes(range(40))                 # 40 литералов: 15 + 25
        src = bytes([0xFF, 25]) + lit + bytes([40, 0, 255, 6]) + bytes([0x00])
        # совпадение 15+4+255+6 = 280 байт со смещением 40, затем пустые литералы
        want = lit + (lit * 8)[:280]
        self.assertEqual(lz4_block_decompress(src), want)

    def test_bad_blocks(self):
        with self.assertRaises(FrameError):
            lz4_block_decompress(bytes([0x10, 0x61, 0x05, 0x00]))   # смещение за начало
        with self.assertRaises(FrameError):
            lz4_block_decompress(bytes([0x50, 0x61]))              # литералы оборваны
        with self.assertRaises(FrameError):
            lz4_block_decompress(bytes([0x15, 0x61, 0x01, 0x00]), max_out=5)

    def test_bound_checked_before_copy(self):
        # битый блок обещает совпадение на 255 000+ байт при пределе 100 -
        # отказ до копирования (замечание Codex), и для литералов тоже
        huge_match = bytes([0x1F, 0x61, 0x01, 0x00]) + bytes([255] * 1000) + bytes([0])
        with self.assertRaises(FrameError):
            lz4_block_decompress(huge_match, max_out=100)
        long_lit = bytes([0xF0, 200 - 15]) + bytes(200)
        with self.assertRaises(FrameError):
            lz4_block_decompress(long_lit, max_out=100)
        self.assertEqual(len(lz4_block_decompress(long_lit, max_out=200)), 200)

    @unittest.skipIf(lz4block is None, "нет библиотеки lz4 для сверки")
    def test_against_reference_lz4(self):
        rnd = random.Random(18112026)
        samples = [b"", b"x", msgpack.packb({"chats": [{"id": i, "title": "Чат %d" % i}
                                                       for i in range(300)]})]
        for _ in range(30):
            alphabet = bytes(rnd.sample(range(256), rnd.randint(2, 40)))
            samples.append(bytes(rnd.choice(alphabet) for _ in range(rnd.randint(1, 5000))))
        for raw in samples:
            comp = lz4block.compress(raw, store_size=False)
            self.assertEqual(lz4_block_decompress(comp), raw)

    @unittest.skipIf(lz4block is None, "нет библиотеки lz4 для сверки")
    def test_compressed_frame(self):
        payload = {"profile": {"contact": {"id": 3562334}}, "chats": [{"id": i} for i in range(200)]}
        body = msgpack.packb(payload)
        comp = lz4block.compress(body, store_size=False)
        factor = -(-len(body) // len(comp))
        got = decode_frame(frame(1, 1, 19, comp, comp=factor))
        self.assertEqual(got["payload"], payload)


if __name__ == "__main__":
    unittest.main()
