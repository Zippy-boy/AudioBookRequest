from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, col, func, select

from app.internal.models import Audiobook, AudiobookRequest, ManualBookRequest
from app.util.db import get_session

from app.routers.api.settings.setup_auth import require_admin_or_setup

router = APIRouter(prefix="/stats")


class StatsResponse(BaseModel):
    requests: int
    downloaded: int
    manual: int
    downloading: int
    attention: int


@router.get("", response_model=StatsResponse)
def get_stats(
    session: Annotated[Session, Depends(get_session)],
    _: Annotated[object, Depends(require_admin_or_setup)],
):
    total_requests = session.exec(select(func.count()).select_from(AudiobookRequest)).one()
    downloaded_books = session.exec(
        select(func.count(Audiobook.asin)).where(Audiobook.downloaded.is_(True))
    ).one()
    active_downloads = session.exec(
        select(func.count(func.distinct(AudiobookRequest.asin)))
        .join(Audiobook)
        .where(
            Audiobook.downloaded.is_(False),
            col(AudiobookRequest.processing_status).in_(
                AudiobookRequest.ACTIVE_DOWNLOAD_STATUSES
            ),
        )
    ).one()
    failed_requests = session.exec(
        select(func.count())
        .select_from(AudiobookRequest)
        .where(col(AudiobookRequest.processing_status).startswith("failed"))
    ).one()
    review_required = session.exec(
        select(func.count())
        .select_from(AudiobookRequest)
        .where(AudiobookRequest.processing_status == "review_required")
    ).one()
    manual_requests = session.exec(
        select(func.count())
        .select_from(ManualBookRequest)
        .where(ManualBookRequest.downloaded.is_(False))
    ).one()
    return StatsResponse(
        requests=total_requests,
        downloaded=downloaded_books,
        manual=manual_requests,
        downloading=active_downloads,
        attention=failed_requests + review_required,
    )
