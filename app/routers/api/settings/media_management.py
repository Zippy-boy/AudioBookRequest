from typing import Annotated

from fastapi import APIRouter, Depends, Response, Security
from pydantic import BaseModel
from sqlmodel import Session

from app.internal.auth.authentication import APIKeyAuth, DetailedUser
from app.internal.media_management.config import media_management_config
from app.internal.models import GroupEnum
from app.util.db import get_session

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
    _: Annotated[DetailedUser, Security(APIKeyAuth(GroupEnum.admin))],
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
    library_path: str | None = None
    folder_pattern: str | None = None
    file_pattern: str | None = None
    use_series_folders: bool | None = None
    use_hardlinks: bool | None = None
    review_before_import: bool | None = None


@router.patch("", status_code=204)
def update_media_management_settings(
    body: UpdateMediaManagementSettings,
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[DetailedUser, Security(APIKeyAuth(GroupEnum.admin))],
):
    if body.library_path is not None:
        media_management_config.set_library_path(session, body.library_path.strip())
    if body.folder_pattern is not None:
        media_management_config.set_folder_pattern(session, body.folder_pattern.strip())
    if body.file_pattern is not None:
        media_management_config.set_file_pattern(session, body.file_pattern.strip())
    if body.use_series_folders is not None:
        media_management_config.set_use_series_folders(session, body.use_series_folders)
    if body.use_hardlinks is not None:
        media_management_config.set_use_hardlinks(session, body.use_hardlinks)
    if body.review_before_import is not None:
        media_management_config.set_review_before_import(
            session, body.review_before_import
        )

    return Response(status_code=204)
