from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, Security, status
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from app.internal.auth.authentication import (
    APIKeyAuth,
    DetailedUser,
    authenticate_user,
    create_user,
    raise_for_invalid_password,
)
from app.internal.auth.config import auth_config
from app.internal.auth.login_types import LoginTypeEnum
from app.internal.auth.ui_sessions import (
    create_ui_session_api_key,
    get_login_lockout_state,
    record_login_failure,
    reset_login_state,
    revoke_ui_session_api_keys,
)
from app.internal.models import GroupEnum, User
from app.util.db import get_session
from app.util.log import logger
from app.internal.setup_state import setup_state

router = APIRouter(prefix="/auth", tags=["Auth"])


class AuthStatus(BaseModel):
    initialized: bool
    login_type: LoginTypeEnum | None
    force_login_type: LoginTypeEnum | None


class AuthUserSummary(BaseModel):
    username: str
    group: GroupEnum
    root: bool


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1)


class LoginResponse(AuthUserSummary):
    apiKey: str


@router.get("/status", response_model=AuthStatus)
def get_auth_status(
    session: Annotated[Session, Depends(get_session)],
):
    login_type_value = auth_config.get(session, "login_type")
    login_type = LoginTypeEnum(login_type_value) if login_type_value else None
    initialized = session.exec(select(User).limit(1)).first() is not None
    try:
        force_login_type = Settings().app.get_force_login_type()
    except ValueError as e:
        logger.error("Invalid force login type", exc_info=e)
        force_login_type = None

    return AuthStatus(
        initialized=initialized,
        login_type=login_type,
        force_login_type=force_login_type,
    )


@router.post("/login", response_model=LoginResponse)
def login(
    body: LoginRequest,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
):
    client_host = request.client.host if request.client else "unknown"
    lockout = get_login_lockout_state(client_host, body.username)
    if lockout.locked:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Please try again later.",
        )

    user = authenticate_user(session, body.username, body.password)
    if not user:
        lockout = record_login_failure(client_host, body.username)
        if lockout.locked:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Please try again later.",
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    reset_login_state(client_host, body.username)
    _, api_key = create_ui_session_api_key(user, session)
    return LoginResponse(
        apiKey=api_key,
        username=user.username,
        group=user.group,
        root=user.root,
    )


@router.get("/me", response_model=AuthUserSummary)
def get_current_user(
    user: Annotated[DetailedUser, Security(APIKeyAuth())],
):
    return AuthUserSummary(username=user.username, group=user.group, root=user.root)


@router.post("/logout", status_code=204)
def logout(
    request: Request,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[DetailedUser, Security(APIKeyAuth())],
):
    revoke_ui_session_api_keys(
        session,
        user.username,
        request.headers.get("authorization"),
    )
    return Response(status_code=204)


class InitializeAuthRequest(BaseModel):
    login_type: LoginTypeEnum
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1)
    confirm_password: str = Field(min_length=1)


@router.post("/initialize", status_code=204)
def initialize_auth(
    body: InitializeAuthRequest,
    session: Annotated[Session, Depends(get_session)],
):
    existing = session.exec(select(User).limit(1)).first()
    if existing:
        raise HTTPException(status_code=409, detail="Already initialized")

    try:
        force_login_type = Settings().app.get_force_login_type()
    except ValueError as e:
        logger.error("Invalid force login type", exc_info=e)
        force_login_type = None
    if force_login_type and body.login_type != force_login_type:
        raise HTTPException(
            status_code=400,
            detail=f"Login type is forced to '{force_login_type.value}'",
        )

    raise_for_invalid_password(session, body.password, body.confirm_password)

    if session.get(User, body.username):
        raise HTTPException(status_code=409, detail="Username already exists")

    user = create_user(
        username=body.username,
        password=body.password,
        group=GroupEnum.admin,
        root=True,
    )
    session.add(user)
    session.commit()

    auth_config.set_login_type(session, body.login_type)
    auth_config.get_auth_secret(session)
    setup_state.mark_complete(session)

    return Response(status_code=204)
