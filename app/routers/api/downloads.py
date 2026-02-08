from typing import Annotated, Literal, Optional

from aiohttp import ClientSession
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Query,
    Response,
)
from pydantic import BaseModel
from sqlmodel import Session, col, delete, select

from app.internal.audiobookshelf.client import background_abs_trigger_scan
from app.internal.audiobookshelf.config import abs_config
from app.internal.book_search import (
    audible_region_type,
    audible_regions,
    get_book_by_asin,
    get_region_from_settings,
)
from app.internal.library.service import library_contains_asin
from app.internal.models import Audiobook, AudiobookRequest, User
from app.internal.prowlarr.prowlarr import start_download
from app.internal.prowlarr.util import ProwlarrMisconfigured, prowlarr_config
from app.internal.query import QueryResult, background_start_query, query_sources
from app.internal.ranking.quality import quality_config
from app.internal.request_logs import log_request_event
from app.internal.system_user import get_system_user
from app.util.connection import get_connection
from app.util.db import get_session
from app.util.log import logger

router = APIRouter(prefix="/downloads", tags=["Downloads"])


class DownloadSourceBody(BaseModel):
    guid: str
    indexer_id: int
    collection: bool = False
    collection_label: str | None = None


class DownloadJob(BaseModel):
    asin: str
    title: str
    subtitle: Optional[str] = None
    status: str
    progress: float
    torrent_hash: Optional[str] = None
    download_state: Optional[str] = None
    downloaded: bool = False


def _job_view(book: Audiobook, req: AudiobookRequest | None) -> DownloadJob:
    status = req.processing_status if req else "not_queued"
    progress = req.download_progress if req else 0.0
    return DownloadJob(
        asin=book.asin,
        title=book.title,
        subtitle=book.subtitle,
        status=status,
        progress=progress,
        torrent_hash=req.torrent_hash if req else None,
        download_state=req.download_state if req else None,
        downloaded=bool(book.downloaded),
    )


def _get_request(session: Session, asin: str) -> AudiobookRequest | None:
    return session.exec(select(AudiobookRequest).where(AudiobookRequest.asin == asin)).first()


@router.post("/{asin}", status_code=201, response_model=DownloadJob)
async def create_download(
    session: Annotated[Session, Depends(get_session)],
    client_session: Annotated[ClientSession, Depends(get_connection)],
    background_task: BackgroundTasks,
    asin: str,
    region: audible_region_type | None = None,
) -> DownloadJob:
    user = get_system_user(session)
    if region is None:
        region = get_region_from_settings()
    if audible_regions.get(region) is None:
        raise HTTPException(status_code=400, detail="Invalid region")

    book = await get_book_by_asin(client_session, asin, region)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    existing_library_book = session.get(Audiobook, asin)
    if existing_library_book:
        book = existing_library_book
    else:
        # Ensure the fetched book is persisted for downstream processing and views
        book = session.merge(book)
        session.commit()
    in_library_on_disk = library_contains_asin(session, asin)

    if in_library_on_disk:
        # Allow re-queue when the book is tracked but not marked downloaded
        if not (existing_library_book and not existing_library_book.downloaded):
            raise HTTPException(
                status_code=400, detail="Book already exists in library folder"
            )

    if existing_library_book and existing_library_book.downloaded:
        raise HTTPException(status_code=400, detail="Book already in library")

    existing_request = _get_request(session, asin)

    if existing_request:
        # Reset stale/old request so it can download again
        existing_request.processing_status = "pending"
        existing_request.download_progress = 0.0
        existing_request.torrent_hash = None
        existing_request.download_state = None
        session.add(existing_request)
        session.commit()
        log_request_event(
            session,
            asin,
            user.username,
            "Download re-opened.",
            commit=True,
        )
        logger.info("Reopened audiobook download", asin=asin)
    else:
        book_request = AudiobookRequest(asin=asin, user_username=user.username)
        session.add(book_request)
        session.commit()
        log_request_event(
            session,
            asin,
            user.username,
            "Download created.",
            commit=True,
        )
        logger.info("Queued new audiobook download", asin=asin)

    if quality_config.get_auto_download(session):
        # Start querying and downloading if auto-download is enabled
        background_task.add_task(
            background_start_query,
            asin=asin,
            requester=User.model_validate(user),
            auto_download=True,
        )

    return _job_view(book, _get_request(session, asin))


@router.get("", response_model=list[DownloadJob])
async def list_downloads(
    session: Annotated[Session, Depends(get_session)],
    filter: Literal["all", "downloaded", "not_downloaded"] = "all",
):
    statement = select(AudiobookRequest, Audiobook).join(
        Audiobook, Audiobook.asin == AudiobookRequest.asin
    )
    if filter == "downloaded":
        statement = statement.where(Audiobook.downloaded.is_(True))
    elif filter == "not_downloaded":
        statement = statement.where(Audiobook.downloaded.is_(False))

    rows = session.exec(statement).all()
    return [_job_view(book, req) for req, book in rows]


@router.get("/{asin}", response_model=DownloadJob)
async def get_download(
    asin: str,
    session: Annotated[Session, Depends(get_session)],
):
    book = session.get(Audiobook, asin)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    req = _get_request(session, asin)
    return _job_view(book, req)


@router.delete("/{asin}")
async def delete_download(
    asin: str,
    session: Annotated[Session, Depends(get_session)],
    delete_files: bool = False,
    delete_files_q: bool | None = Query(default=None),
):
    from app.internal.download_clients.qbittorrent import QbittorrentClient
    from app.internal.download_clients.config import download_client_config

    if delete_files_q is not None:
        delete_files = delete_files_q

    # Try to delete from qBittorrent if enabled
    if download_client_config.get_qbit_enabled(session):
        try:
            client = QbittorrentClient(session)
            torrents = await client.get_torrents()
            for t in torrents:
                if f"asin:{asin}" in t.get("tags", ""):
                    await client.delete_torrents([t["hash"]], delete_files=delete_files)
                    logger.info(
                        "Deleted torrent from qBittorrent for deleted download",
                        asin=asin,
                        hash=t["hash"],
                    )
        except Exception as e:
            logger.warning(
                "Failed to delete torrent from qBittorrent during download deletion",
                asin=asin,
                error=str(e),
            )

    session.execute(delete(AudiobookRequest).where(col(AudiobookRequest.asin) == asin))
    session.commit()

    # Trigger ABS rescan to drop removed items when files were deleted
    if delete_files and abs_config.is_valid(session):
        await background_abs_trigger_scan()

    return Response(status_code=204)


@router.post(
    "/{asin}/refresh",
    description="Refresh the sources from Prowlarr for a book",
)
async def refresh_source(
    asin: str,
    session: Annotated[Session, Depends(get_session)],
    client_session: Annotated[ClientSession, Depends(get_connection)],
    force_refresh: bool = False,
):
    user = get_system_user(session)
    await query_sources(
        asin=asin,
        session=session,
        client_session=client_session,
        force_refresh=force_refresh,
        requester=User.model_validate(user),
    )
    return Response(status_code=202)


@router.get("/{asin}/sources", response_model=QueryResult)
async def list_sources(
    asin: str,
    session: Annotated[Session, Depends(get_session)],
    client_session: Annotated[ClientSession, Depends(get_connection)],
    only_cached: bool = False,
    page: int = 0,
):
    try:
        prowlarr_config.raise_if_invalid(session)
    except ProwlarrMisconfigured:
        raise HTTPException(status_code=400, detail="Prowlarr misconfigured")

    user = get_system_user(session)
    result = await query_sources(
        asin,
        session=session,
        client_session=client_session,
        requester=User.model_validate(user),
        only_return_if_cached=only_cached,
        page=page,
    )
    return result


@router.post("/{asin}/download")
async def start_download_source(
    asin: str,
    background_task: BackgroundTasks,
    body: DownloadSourceBody,
    session: Annotated[Session, Depends(get_session)],
    client_session: Annotated[ClientSession, Depends(get_connection)],
):
    user = get_system_user(session)
    book_request = session.exec(
        select(AudiobookRequest)
        .join(Audiobook)
        .where(
            AudiobookRequest.asin == asin,
            Audiobook.downloaded.is_(False),
        )
    ).first()

    if not book_request:
        raise HTTPException(
            status_code=404, detail="Active download for this audiobook not found."
        )

    try:
        from app.internal.prowlarr.util import prowlarr_source_cache
        from app.internal.prowlarr.prowlarr import build_prowlarr_query

        book = session.get(Audiobook, asin)
        cache_key = build_prowlarr_query(session, book) if book else ""
        sources = prowlarr_source_cache.get(
            prowlarr_config.get_source_ttl(session), cache_key
        )
        source = None
        if sources:
            source = next(
                (
                    s
                    for s in sources
                    if s.guid == body.guid and s.indexer_id == body.indexer_id
                ),
                None,
            )

        if not source:
            raise HTTPException(
                status_code=404, detail="Source not found in cache for this book."
            )

        success = await start_download(
            session=session,
            client_session=client_session,
            guid=body.guid,
            indexer_id=body.indexer_id,
            requester=user,
            audiobook_request=book_request,
            prowlarr_source=source,
            collection=body.collection,
            collection_label=body.collection_label,
        )
    except ProwlarrMisconfigured as e:
        logger.error("Prowlarr misconfigured for download", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

    if not success:
        logger.error("Failed to start download via qBittorrent", asin=asin)
        raise HTTPException(status_code=500, detail="Failed to start download")

    if abs_config.is_valid(session):
        background_task.add_task(background_abs_trigger_scan)

    return Response(status_code=204)


@router.post("/{asin}/auto-download")
async def start_auto_download_endpoint(
    asin: str,
    session: Annotated[Session, Depends(get_session)],
    client_session: Annotated[ClientSession, Depends(get_connection)],
):
    user = get_system_user(session)
    log_request_event(
        session,
        asin,
        user.username,
        "Auto-download queued.",
        commit=True,
    )
    await query_sources(
        asin=asin,
        start_auto_download=True,
        session=session,
        client_session=client_session,
        requester=user,
    )
    return Response(status_code=204)
