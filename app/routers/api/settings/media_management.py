from typing import Annotated

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel
from sqlmodel import Session

from app.internal.media_management.config import media_management_config
from app.util.db import get_session

from app.routers.api.settings.setup_auth import require_admin_or_setup

router = APIRouter(prefix="/media-management")


class MediaManagementSettings(BaseModel):
    library_path: str
    folder_pattern: str
    file_pattern: str
    use_series_folders: bool
    use_hardlinks: bool
    review_before_import: bool


@router.get("", response_model=MediaManagementSettings)
def get_media_management_settings(
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[object, Depends(require_admin_or_setup)],
):
    return MediaManagementSettings(
        library_path=media_management_config.get_library_path(session) or "",
        folder_pattern=media_management_config.get_folder_pattern(session),
        file_pattern=media_management_config.get_file_pattern(session),
        use_series_folders=media_management_config.get_use_series_folders(session),
        use_hardlinks=media_management_config.get_use_hardlinks(session),
        review_before_import=media_management_config.get_review_before_import(session),
    )


class UpdateMediaManagementSettings(BaseModel):
    library_path: str
    folder_pattern: str
    file_pattern: str
    use_series_folders: bool
    use_hardlinks: bool
    review_before_import: bool


@router.patch("", status_code=204)
def update_media_management_settings(
    body: UpdateMediaManagementSettings,
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[object, Depends(require_admin_or_setup)],
):
    media_management_config.set_library_path(session, body.library_path)
    media_management_config.set_folder_pattern(session, body.folder_pattern)
    media_management_config.set_file_pattern(session, body.file_pattern)
    media_management_config.set_use_series_folders(session, body.use_series_folders)
    media_management_config.set_use_hardlinks(session, body.use_hardlinks)
    media_management_config.set_review_before_import(session, body.review_before_import)
    return Response(status_code=204)
