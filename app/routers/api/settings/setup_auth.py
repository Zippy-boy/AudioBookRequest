from typing import Annotated

from fastapi import Depends, Request
from sqlmodel import Session

from app.internal.auth.authentication import APIKeyAuth, DetailedUser
from app.internal.auth.login_types import LoginTypeEnum
from app.internal.env_settings import Settings
from app.internal.models import GroupEnum, User
from app.internal.setup_state import setup_state
from app.util.db import get_session


async def require_admin_or_setup(
    request: Request,
    session: Annotated[Session, Depends(get_session)],
) -> DetailedUser:
    if Settings().app.force_setup_wizard or setup_state.requires_setup(session):
        existing_user = session.get(User, "__setup__")
        if existing_user is None:
            existing_user = User(
                username="__setup__",
                password="",
                group=GroupEnum.admin,
                root=True,
            )
        return DetailedUser.model_validate(
            existing_user, update={"login_type": LoginTypeEnum.none}
        )

    return await APIKeyAuth(GroupEnum.admin)(request, session)
