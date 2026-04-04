from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlmodel import Session, select

from app.internal.auth.authentication import (
    APIKeyAuth,
    authenticate_user,
    create_api_key,
    create_user,
    ph,
    raise_for_invalid_password,
)
from app.internal.auth.config import auth_config
from app.internal.env_settings import Settings
from app.internal.auth.login_types import LoginTypeEnum
from app.internal.models import APIKey, GroupEnum, User
from app.util.db import get_session

router = APIRouter(prefix="/auth", tags=["Auth"])


class AuthStatus(BaseModel):
    initialized: bool
    login_type: str | None = None
    force_login_type: str | None = None


@router.get("/status", response_model=AuthStatus)
def get_auth_status(session: Annotated[Session, Depends(get_session)]):
    first_user = session.exec(select(User).limit(1)).first()
    try:
        forced_login_type = Settings().app.get_force_login_type()
    except ValueError:
        forced_login_type = None
    login_type = auth_config.get(session, "login_type")
    return AuthStatus(
        initialized=first_user is not None,
        login_type=login_type,
        force_login_type=forced_login_type.value if forced_login_type else None,
    )


class InitializeAuthBody(BaseModel):
    login_type: LoginTypeEnum
    username: str
    password: str
    confirm_password: str


@router.post("/initialize", status_code=204)
def initialize_auth(
    body: InitializeAuthBody,
    session: Annotated[Session, Depends(get_session)],
):
    existing_user = session.exec(select(User).limit(1)).first()
    if existing_user is not None:
        raise HTTPException(status_code=400, detail="Authentication is already initialized")

    if not body.username.strip():
        raise HTTPException(status_code=400, detail="Username is required")

    raise_for_invalid_password(session, body.password, body.confirm_password)
    user = create_user(
        username=body.username.strip(),
        password=body.password,
        group=GroupEnum.admin,
        root=True,
    )
    session.add(user)
    auth_config.set_login_type(session, body.login_type)
    session.commit()


class LoginBody(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    api_key: str


@router.post("/login", response_model=LoginResponse)
def api_login(
    body: LoginBody,
    session: Annotated[Session, Depends(get_session)],
):
    user = authenticate_user(session, body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    api_key, private_key = create_api_key(user, "UI Session")
    session.add(api_key)
    session.commit()
    return LoginResponse(api_key=private_key)


@router.post("/logout", status_code=204)
def api_logout(
    request: Request,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(APIKeyAuth())],
):
    authorization = request.headers.get("Authorization", "")
    token = authorization.removeprefix("Bearer").strip()
    if token:
        api_keys = session.exec(
            select(APIKey).where(APIKey.user_username == user.username)
        ).all()
        for api_key in api_keys:
            try:
                if ph.verify(api_key.key_hash, token):
                    session.delete(api_key)
                    session.commit()
                    break
            except Exception:
                continue
    return Response(status_code=204)
