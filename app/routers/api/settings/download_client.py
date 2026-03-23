from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, Security
from pydantic import BaseModel
from sqlmodel import Session

from app.internal.auth.authentication import APIKeyAuth, DetailedUser
from app.internal.download_clients.config import download_client_config
from app.internal.models import GroupEnum
from app.util.db import get_session

router = APIRouter(prefix="/download-client")


class DownloadClientSettings(BaseModel):
    qbit_base_url: str
    qbit_user: str
    qbit_pass: str
    qbit_category: str
    qbit_save_path: str
    qbit_enabled: bool
    qbit_complete_action: str


@router.get("", response_model=DownloadClientSettings)
def get_download_client_settings(
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[DetailedUser, Security(APIKeyAuth(GroupEnum.admin))],
):
    return DownloadClientSettings(
        qbit_base_url=download_client_config.get_qbit_base_url(session),
        qbit_user=download_client_config.get_qbit_user(session) or "",
        qbit_pass=download_client_config.get_qbit_pass(session) or "",
        qbit_category=download_client_config.get_qbit_category(session),
        qbit_save_path=download_client_config.get_qbit_save_path(session) or "",
        qbit_enabled=download_client_config.get_qbit_enabled(session),
        qbit_complete_action=download_client_config.get_qbit_complete_action(session),
    )


class UpdateDownloadClientSettings(BaseModel):
    qbit_base_url: str | None = None
    qbit_user: str | None = None
    qbit_pass: str | None = None
    qbit_category: str | None = None
    qbit_save_path: str | None = None
    qbit_enabled: bool | None = None
    qbit_complete_action: str | None = None


@router.patch("", status_code=204)
def update_download_client_settings(
    body: UpdateDownloadClientSettings,
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[DetailedUser, Security(APIKeyAuth(GroupEnum.admin))],
):
    if body.qbit_base_url is not None:
        download_client_config.set_qbit_base_url(session, body.qbit_base_url.strip())
    if body.qbit_user is not None:
        download_client_config.set_qbit_user(session, body.qbit_user.strip())
    if body.qbit_pass is not None:
        download_client_config.set_qbit_pass(session, body.qbit_pass)
    if body.qbit_category is not None:
        download_client_config.set_qbit_category(session, body.qbit_category.strip())
    if body.qbit_save_path is not None:
        download_client_config.set_qbit_save_path(session, body.qbit_save_path.strip())
    if body.qbit_enabled is not None:
        download_client_config.set_qbit_enabled(session, body.qbit_enabled)
    if body.qbit_complete_action is not None:
        action = body.qbit_complete_action.strip()
        if action not in {"copy", "move", "hardlink"}:
            raise HTTPException(
                status_code=400,
                detail="qbit_complete_action must be one of: copy, move, hardlink",
            )
        download_client_config.set_qbit_complete_action(session, action)

    return Response(status_code=204)
