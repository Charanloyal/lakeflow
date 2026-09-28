"""Authentication: HTTP Basic for API clients, signed HttpOnly session cookie for the UI, admin role for writes."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request

from .domain.sessions import Identity, check_credentials, parse_basic, verify_session

SESSION_COOKIE = "lakeflow_session"
CSRF_HEADER = "x-lakeflow-csrf"
UNSAFE = {"POST", "PUT", "PATCH", "DELETE"}


def current_identity(request: Request) -> Identity:
    ctx = request.app.state.ctx
    header = request.headers.get("authorization")
    if header:
        credentials = parse_basic(header)
        identity = credentials and check_credentials(credentials[0], credentials[1], ctx.settings.users)
        if not identity:
            # No WWW-Authenticate header: browsers must never cache Basic credentials (that would make them ambient).
            raise HTTPException(401, "invalid credentials")
        return identity
    token = request.cookies.get(SESSION_COOKIE)
    identity = verify_session(token, ctx.settings.session_secret) if token else None
    if identity is None:
        raise HTTPException(401, "authentication required")
    if request.method in UNSAFE and request.headers.get(CSRF_HEADER) != "1":
        raise HTTPException(403, f"missing {CSRF_HEADER} header for cookie-authenticated write")
    return identity


def require_viewer(identity: Identity = Depends(current_identity)) -> Identity:
    return identity


def require_admin(identity: Identity = Depends(current_identity)) -> Identity:
    if not identity.is_admin:
        raise HTTPException(403, "admin role required")
    return identity
