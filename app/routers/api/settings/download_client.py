from typing import Annotated

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel
from sqlmodel import Session

from app.internal.download_clients.config import download_client_config
from app.util.db import get_session

from app.routers.api.settings.setup_auth import require_admin_or_setup

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
    _: Annotated[object, Depends(require_admin_or_setup)],
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
    qbit_base_url: str
    qbit_user: str
    qbit_pass: str
    qbit_category: str
    qbit_save_path: str
    qbit_enabled: bool
    qbit_complete_action: str


@router.patch("", status_code=204)
def update_download_client_settings(
    body: UpdateDownloadClientSettings,
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[object, Depends(require_admin_or_setup)],
):
    download_client_config.set_qbit_base_url(session, body.qbit_base_url)
    download_client_config.set_qbit_user(session, body.qbit_user)
    if body.qbit_pass:
        download_client_config.set_qbit_pass(session, body.qbit_pass)
    download_client_config.set_qbit_category(session, body.qbit_category)
    download_client_config.set_qbit_save_path(session, body.qbit_save_path)
    download_client_config.set_qbit_enabled(session, body.qbit_enabled)
    download_client_config.set_qbit_complete_action(session, body.qbit_complete_action)
    return Response(status_code=204)
