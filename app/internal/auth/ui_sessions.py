from __future__ import annotations

import secrets
import threading
import time
from typing import NamedTuple
from uuid import UUID

from argon2.exceptions import VerifyMismatchError
from sqlmodel import Session, select

from app.internal.auth.authentication import create_api_key, ph
from app.internal.models import APIKey, User

UI_SESSION_KEY_PREFIX = "ui-session-"
UI_SESSION_KEY_LIMIT = 5
LOGIN_ATTEMPT_WINDOW_SECONDS = 10 * 60
LOGIN_LOCKOUT_SECONDS = 15 * 60
LOGIN_FAILURE_LIMIT = 5

_state_lock = threading.Lock()
_attempts: dict[tuple[str, str], list[float]] = {}
_lockouts: dict[tuple[str, str], float] = {}


class LoginLockoutState(NamedTuple):
    locked: bool
    seconds_remaining: int | None = None


def _attempt_key(ip_address: str, username: str) -> tuple[str, str]:
    return ip_address, username


def _prune_attempts(attempts: list[float], now: float) -> list[float]:
    cutoff = now - LOGIN_ATTEMPT_WINDOW_SECONDS
    return [attempt for attempt in attempts if attempt >= cutoff]


def get_login_lockout_state(ip_address: str, username: str) -> LoginLockoutState:
    key = _attempt_key(ip_address, username)
    now = time.monotonic()

    with _state_lock:
        lockout_until = _lockouts.get(key)
        if lockout_until is None:
            return LoginLockoutState(False, None)

        if lockout_until <= now:
            _lockouts.pop(key, None)
            _attempts.pop(key, None)
            return LoginLockoutState(False, None)

        return LoginLockoutState(True, int(lockout_until - now) + 1)


def record_login_failure(ip_address: str, username: str) -> LoginLockoutState:
    key = _attempt_key(ip_address, username)
    now = time.monotonic()

    with _state_lock:
        if (lockout_until := _lockouts.get(key)) is not None:
            if lockout_until > now:
                return LoginLockoutState(True, int(lockout_until - now) + 1)
            _lockouts.pop(key, None)
            _attempts.pop(key, None)

        attempts = _prune_attempts(_attempts.get(key, []), now)
        attempts.append(now)

        if len(attempts) >= LOGIN_FAILURE_LIMIT:
            _attempts.pop(key, None)
            _lockouts[key] = now + LOGIN_LOCKOUT_SECONDS
            return LoginLockoutState(True, LOGIN_LOCKOUT_SECONDS)

        _attempts[key] = attempts
        return LoginLockoutState(False, None)


def reset_login_state(ip_address: str, username: str) -> None:
    key = _attempt_key(ip_address, username)
    with _state_lock:
        _attempts.pop(key, None)
        _lockouts.pop(key, None)


def _ui_session_key_name() -> str:
    return f"{UI_SESSION_KEY_PREFIX}{secrets.token_hex(8)}"


def create_ui_session_api_key(user: User, session: Session) -> tuple[APIKey, str]:
    api_key, private_key = create_api_key(user, _ui_session_key_name())
    session.add(api_key)
    session.commit()
    prune_ui_session_api_keys(session, user.username, keep_api_key_id=api_key.id)
    return api_key, private_key


def _select_ui_session_api_keys(session: Session, username: str) -> list[APIKey]:
    return session.exec(
        select(APIKey).where(
            APIKey.user_username == username,
            APIKey.name.like(f"{UI_SESSION_KEY_PREFIX}%"),
        )
    ).all()


def prune_ui_session_api_keys(
    session: Session,
    username: str,
    keep_api_key_id: UUID | None = None,
) -> None:
    api_keys = _select_ui_session_api_keys(session, username)
    if len(api_keys) <= UI_SESSION_KEY_LIMIT:
        return

    api_keys.sort(key=lambda api_key: (api_key.created_at, api_key.id))
    if keep_api_key_id is not None:
        candidate_keys = [api_key for api_key in api_keys if api_key.id != keep_api_key_id]
    else:
        candidate_keys = api_keys

    to_delete = len(api_keys) - UI_SESSION_KEY_LIMIT
    if to_delete <= 0:
        return

    for api_key in candidate_keys[:to_delete]:
        session.delete(api_key)
    session.commit()


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return None

    token = token.strip()
    return token or None


def find_current_ui_session_api_key(
    session: Session,
    username: str,
    authorization: str | None,
) -> APIKey | None:
    token = _extract_bearer_token(authorization)
    if not token:
        return None

    for api_key in _select_ui_session_api_keys(session, username):
        try:
            if ph.verify(api_key.key_hash, token):
                return api_key
        except VerifyMismatchError:
            continue

    return None


def revoke_ui_session_api_keys(
    session: Session,
    username: str,
    authorization: str | None,
) -> None:
    api_key = find_current_ui_session_api_key(session, username, authorization)
    if api_key is not None:
        session.delete(api_key)
        session.commit()
        return

    for candidate in _select_ui_session_api_keys(session, username):
        session.delete(candidate)
    session.commit()
