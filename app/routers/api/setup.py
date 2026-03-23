from typing import Annotated

from fastapi import APIRouter, Depends, Response, Security
from pydantic import BaseModel
from sqlmodel import Session

from app.internal.auth.authentication import APIKeyAuth, DetailedUser
from app.internal.auth.authentication import GroupEnum
from app.internal.env_settings import Settings
from app.internal.setup_state import setup_state
from app.util.db import get_session


class SetupStatus(BaseModel):
    setup_required: bool
    setup_complete: bool
    force_setup: bool


router = APIRouter(prefix="/setup", tags=["Setup"])


@router.get("", response_model=SetupStatus)
def get_setup_status(session: Annotated[Session, Depends(get_session)]):
    settings = Settings()
    force = settings.app.force_setup_wizard
    complete = setup_state.is_complete(session)
    return SetupStatus(
        setup_required=force or not complete,
        setup_complete=complete,
        force_setup=force,
    )


@router.post("/complete", status_code=204)
def complete_setup(
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[DetailedUser, Security(APIKeyAuth())],
):
    setup_state.mark_complete(session)
    return Response(status_code=204)


@router.post("/reset", status_code=204)
def reset_setup(
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[DetailedUser, Security(APIKeyAuth(GroupEnum.admin))],
):
    setup_state.reset(session)
    return Response(status_code=204)
