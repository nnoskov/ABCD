"""Минимальный UDP-клиент передачи данных извлечённой партии.

Использует только стандартную библиотеку. Логическая полезная нагрузка имеет вид:
    rpn=...&rpd=...&name=...&pn=...&qty=...&ok=...&nok=...

Каждая UDP-дейтаграмма получает текстовый заголовок с идентификатором
сообщения и номером пакета. Полезная строка передаётся непосредственно
после заголовка и при необходимости разбивается на несколько частей.
"""

from __future__ import annotations

import ipaddress
import math
import socket
import uuid
from dataclasses import dataclass
from urllib.parse import urlencode


# 576 байт — минимальный размер IPv4-пакета, который хост должен уметь
# принимать. С учётом максимально длинного IPv4-заголовка (60 байт)
# и UDP-заголовка (8 байт) остаётся 508 байт UDP-полезной нагрузки.
MAX_SAFE_UDP_DATAGRAM_BYTES = 508
PACKET_PROTOCOL = "PSUDP/1"


@dataclass(frozen=True)
class UdpTransferResult:
    message_id: str
    packet_count: int
    bytes_sent: int
    logical_payload: str


def _validated_destination(server_ip: str, server_port: int):
    ip = ipaddress.ip_address(str(server_ip).strip())

    if ip.is_unspecified:
        raise ValueError("UDP server IP must not be unspecified")
    if ip.is_multicast:
        raise ValueError("UDP server IP must not be multicast")

    if isinstance(server_port, bool):
        raise ValueError("UDP server port must be an integer")

    port = int(server_port)
    if not 1 <= port <= 65535:
        raise ValueError("UDP server port must be in range 1..65535")

    family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
    destination = (str(ip), port, 0, 0) if ip.version == 6 else (str(ip), port)
    return family, destination


def build_batch_payload(
    *,
    passport_number,
    passport_date,
    product_name,
    product_code,
    product_count,
    measured_good,
    measured_bad,
) -> str:
    """Формирует логическую строку в фиксированном порядке полей."""
    return urlencode(
        [
            ("rpn", str(passport_number or "")),
            ("rpd", str(passport_date or "")),
            ("name", str(product_name or "")),
            ("pn", str(product_code or "")),
            ("qty", int(product_count or 0)),
            ("ok", int(measured_good or 0)),
            ("nok", int(measured_bad or 0)),
        ]
    )


def _packet_header(message_id: str, packet_no: int, packet_total: int) -> bytes:
    return (
        f"{PACKET_PROTOCOL} id={message_id} "
        f"no={int(packet_no)} total={int(packet_total)}\n"
    ).encode("ascii")


def build_numbered_datagrams(
    logical_payload: str,
    *,
    message_id: str | None = None,
) -> tuple[str, list[bytes]]:
    """Разбивает сообщение на нумерованные дейтаграммы не более 508 байт."""
    msg_id = str(message_id or uuid.uuid4().hex).strip()
    if not msg_id or any(
        ch not in "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_"
        for ch in msg_id
    ):
        raise ValueError("message_id contains unsupported characters")

    # urlencode формирует ASCII-строку: национальные символы представлены
    # процентным кодированием и могут безопасно разбиваться между пакетами.
    payload_bytes = str(logical_payload).encode("ascii")

    chunk_size = max(
        1,
        MAX_SAFE_UDP_DATAGRAM_BYTES
        - len(_packet_header(msg_id, 1, 1)),
    )

    # Размер заголовка зависит от количества цифр в total/no.
    for _ in range(8):
        packet_total = max(1, math.ceil(len(payload_bytes) / chunk_size))
        max_header_size = len(
            _packet_header(msg_id, packet_total, packet_total)
        )
        next_chunk_size = MAX_SAFE_UDP_DATAGRAM_BYTES - max_header_size
        if next_chunk_size <= 0:
            raise ValueError("UDP packet header exceeds safe datagram size")
        if next_chunk_size == chunk_size:
            break
        chunk_size = next_chunk_size

    chunks = [
        payload_bytes[pos : pos + chunk_size]
        for pos in range(0, len(payload_bytes), chunk_size)
    ] or [b""]

    packet_total = len(chunks)
    packets = [
        _packet_header(msg_id, packet_no, packet_total) + chunk
        for packet_no, chunk in enumerate(chunks, start=1)
    ]

    if any(len(packet) > MAX_SAFE_UDP_DATAGRAM_BYTES for packet in packets):
        raise ValueError("generated UDP datagram exceeds safe size")

    return msg_id, packets


def send_batch_data(
    *,
    server_ip: str,
    server_port: int,
    passport_number,
    passport_date,
    product_name,
    product_code,
    product_count,
    measured_good,
    measured_bad,
) -> UdpTransferResult:
    """Отправляет данные партии без ожидания ACK и без повторов."""
    family, destination = _validated_destination(server_ip, server_port)
    logical_payload = build_batch_payload(
        passport_number=passport_number,
        passport_date=passport_date,
        product_name=product_name,
        product_code=product_code,
        product_count=product_count,
        measured_good=measured_good,
        measured_bad=measured_bad,
    )
    message_id, packets = build_numbered_datagrams(logical_payload)

    bytes_sent = 0
    with socket.socket(family, socket.SOCK_DGRAM) as sock:
        sock.settimeout(0.2)
        for packet in packets:
            sent = sock.sendto(packet, destination)
            if sent != len(packet):
                raise OSError(
                    f"partial UDP send: {sent} of {len(packet)} bytes"
                )
            bytes_sent += sent

    return UdpTransferResult(
        message_id=message_id,
        packet_count=len(packets),
        bytes_sent=bytes_sent,
        logical_payload=logical_payload,
    )
