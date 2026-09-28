"""PostgreSQL log sequence number helpers (Debezium reports LSNs as 64-bit integers)."""

from __future__ import annotations


def lsn_to_int(text: str) -> int:
    """'16/B374D848' -> 97500059720."""
    high, low = text.strip().upper().split("/")
    return (int(high, 16) << 32) + int(low, 16)


def int_to_lsn(value: int) -> str:
    if value < 0:
        raise ValueError("LSN cannot be negative")
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"
