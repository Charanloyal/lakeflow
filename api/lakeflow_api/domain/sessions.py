"""Signed session tokens (HMAC-SHA256) and constant-time credential checks."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass

ROLES = ("viewer", "admin")


@dataclass(frozen=True)
class Identity:
    user: str
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def sign_session(identity: Identity, secret: str, ttl_s: int = 8 * 3600, now: float | None = None) -> str:
    payload = {"u": identity.user, "r": identity.role, "exp": int((now or time.time()) + ttl_s)}
    body = _b64(json.dumps(payload, separators=(",", ":")).encode())
    mac = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64(mac)}"


def verify_session(token: str, secret: str, now: float | None = None) -> Identity | None:
    try:
        body, mac = token.split(".", 1)
        expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _unb64(mac)):
            return None
        payload = json.loads(_unb64(body))
    except (ValueError, json.JSONDecodeError):
        return None
    if payload.get("r") not in ROLES or payload.get("exp", 0) < (now or time.time()):
        return None
    return Identity(payload["u"], payload["r"])


def check_credentials(username: str, password: str, users: dict[str, tuple[str, str]]) -> Identity | None:
    """users: username -> (password, role). Constant-time compare even for unknown users."""
    stored = users.get(username)
    expected_password = stored[0] if stored else "\x00invalid"
    ok = hmac.compare_digest(password.encode(), expected_password.encode())
    if stored is None or not ok:
        return None
    return Identity(username, stored[1])


def parse_basic(header: str) -> tuple[str, str] | None:
    if not header.lower().startswith("basic "):
        return None
    try:
        decoded = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None
    user, sep, password = decoded.partition(":")
    return (user, password) if sep else None
