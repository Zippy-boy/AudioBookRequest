from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session

from app.internal.env_settings import Settings
from app.internal.setup_state import setup_state
from app.util.db import get_session

router = APIRouter(prefix="/setup", tags=["Setup"])


class SetupStatus(BaseModel):
    setup_required: bool
    setup_complete: bool
    force_setup: bool


class SetupOverview(BaseModel):
    auth_initialized: bool
    prowlarr_ready: bool
    download_client_ready: bool
    media_management_ready: bool
    audiobookshelf_ready: bool
    setup_complete: bool


def is_setup_ready(overview: SetupOverview) -> bool:
    return (
        overview.auth_initialized
        and overview.prowlarr_ready
        and overview.download_client_ready
        and overview.media_management_ready
        and overview.audiobookshelf_ready
    )


def _build_setup_overview(session: Session) -> SetupOverview:
    auth_initialized = setup_state.is_auth_initialized(session)
    prowlarr_ready = setup_state.is_prowlarr_ready(session)
    download_client_ready = setup_state.is_download_client_ready(session)
    media_management_ready = setup_state.is_media_management_ready(session)
    audiobookshelf_ready = setup_state.is_audiobookshelf_ready(session)
    setup_complete = setup_state.is_complete(session)
    if setup_complete and setup_state.requires_setup(session):
        setup_complete = False

    return SetupOverview(
        auth_initialized=auth_initialized,
        prowlarr_ready=prowlarr_ready,
        download_client_ready=download_client_ready,
        media_management_ready=media_management_ready,
        audiobookshelf_ready=audiobookshelf_ready,
        setup_complete=setup_complete,
    )


@router.get("", response_model=SetupStatus)
def get_setup_status(session: Annotated[Session, Depends(get_session)]):
    overview = _build_setup_overview(session)
    force_setup = Settings().app.force_setup_wizard
    return SetupStatus(
        setup_required=force_setup or setup_state.requires_setup(session),
        setup_complete=overview.setup_complete,
        force_setup=force_setup,
    )


@router.get("/overview", response_model=SetupOverview)
def get_setup_overview(session: Annotated[Session, Depends(get_session)]):
    return _build_setup_overview(session)


@router.post("/complete", status_code=204)
def complete_setup(session: Annotated[Session, Depends(get_session)]):
    overview = _build_setup_overview(session)
    if not is_setup_ready(overview):
        raise HTTPException(status_code=400, detail="Setup is not complete")
    setup_state.mark_complete(session)
    return None
