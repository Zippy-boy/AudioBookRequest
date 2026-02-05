import os
import shutil
from typing import Optional
from sqlmodel import Session
from app.internal.models import (
    Audiobook,
    AudiobookRequest,
    LibraryImportSession,
    ImportSessionStatus,
    RequestLogLevel,
)
from app.internal.request_logs import log_request_event
from app.internal.media_management.config import media_management_config
from app.internal.library.service import (
    get_book_folder_path,
    generate_audiobook_filename,
)
from app.util.log import logger
from app.util.sort import natural_sort
from app.internal.metadata import generate_abs_metadata, generate_opf_metadata
from app.internal.library.scanner import LibraryScanner
import aiohttp


def smart_copy(
    src: str, dst: str, use_hardlinks: bool = False, delete_source: bool = False
):
    """
    Copies (or hardlinks) a file to a new destination.
    If delete_source is True, it effectively 'moves' the file.
    """
    # Safety: Don't do anything if source and destination are the same
    if os.path.abspath(src) == os.path.abspath(dst):
        return

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    if use_hardlinks:
        try:
            os.link(src, dst)
            if delete_source:
                os.remove(src)
            return
        except Exception as e:
            logger.debug(
                "Processor: Hardlink failed, falling back to copy", error=str(e)
            )

    shutil.copy2(src, dst)
    if delete_source:
        os.remove(src)


async def process_completed_download(
    session: Session,
    book_request: AudiobookRequest,
    download_path: str,
    delete_source: bool = False,
    collection: bool = False,
    collection_label: str | None = None,
):
    """
    Takes a completed download, organizes it into the library, and generates metadata.
    """
    book = session.get(Audiobook, book_request.asin)
    if not book:
        logger.error("Processor: Book not found in database", asin=book_request.asin)
        return

    lib_root = media_management_config.get_library_path(session)
    if not lib_root:
        logger.error("Processor: Library path not configured")
        return

    # If this was a collection torrent, trigger an import scan of the download folder
    if collection:
        log_request_event(
            session,
            book_request.asin,
            book_request.user_username,
            f"Collection download completed. Scanning folder for books ({collection_label or 'collection'})",
            commit=False,
        )
        session.commit()

        import_session = LibraryImportSession(
            root_path=download_path, status=ImportSessionStatus.scanning
        )
        session.add(import_session)
        session.commit()
        session.refresh(import_session)

        try:
            async with aiohttp.ClientSession() as client_session:
                scanner = LibraryScanner(import_session.id)
                await scanner.scan(client_session)
            session.refresh(import_session)
            log_request_event(
                session,
                book_request.asin,
                book_request.user_username,
                "Collection scan completed. Review/import items in Library > Import.",
                commit=False,
            )
        except Exception as e:
            import_session.status = ImportSessionStatus.failed
            session.add(import_session)
            session.commit()
            logger.error("Processor: Collection scan failed", error=str(e))
            log_request_event(
                session,
                book_request.asin,
                book_request.user_username,
                f"Collection scan failed: {str(e)}",
                level=RequestLogLevel.error,
                commit=False,
            )

        book_request.processing_status = "completed"
        session.add(book_request)
        session.commit()
        return

    author = book.authors[0] if book.authors else "Unknown"
    # series = book.series[0] if book.series else None
    year = book.release_date.year if book.release_date else "Unknown"

    folder_rel_path = get_book_folder_path(session, book)
    if not folder_rel_path:
        folder_rel_path = f"{author}/{book.title} ({year})"

    dest_path = os.path.join(lib_root, folder_rel_path)
    os.makedirs(dest_path, exist_ok=True)

    # 2. Organize files
    from app.internal.download_clients.config import download_client_config

    complete_action = download_client_config.get_qbit_complete_action(session)
    use_hardlinks = complete_action == "hardlink"
    delete_source = delete_source or complete_action == "move"
    if collection:
        # Keep collection payload in place; copy/hardlink only
        delete_source = False
    file_pattern = media_management_config.get_file_pattern(session)
    logger.info(
        "Processor: Organizing and renaming files",
        dest=dest_path,
        hardlinks=use_hardlinks,
        delete_source=delete_source,
        complete_action=complete_action,
        collection=collection,
        collection_label=collection_label,
    )

    book_request.processing_status = "organizing_files"
    log_request_event(
        session,
        book_request.asin,
        book_request.user_username,
        "Organizing and renaming files.",
        commit=False,
    )
    session.add(book_request)
    session.commit()

    source_paths = download_path.split("|")
    audio_files_to_process = []
    if len(source_paths) == 1 and os.path.isdir(source_paths[0]):
        for root, dirs, files in os.walk(source_paths[0]):
            for file in files:
                if any(
                    file.lower().endswith(ext)
                    for ext in [
                        ".m4b",
                        ".mp3",
                        ".m4a",
                        ".flac",
                        ".wav",
                        ".ogg",
                        ".opus",
                        ".aac",
                        ".wma",
                    ]
                ):
                    audio_files_to_process.append(os.path.join(root, file))
        natural_sort(audio_files_to_process)
    else:
        audio_files_to_process = [
            p for p in source_paths if os.path.exists(p) and not os.path.isdir(p)
        ]

    mam_result = None
    if book_request.mam_id:
        try:
            from app.internal.indexers.mam import (
                fetch_mam_book_details,
                MamIndexer,
                ValuedMamConfigurations,
                SessionContainer,
            )
            from app.internal.indexers.configuration import create_valued_configuration
            from app.internal.book_search import _normalize_series
            import aiohttp

            async with aiohttp.ClientSession() as client_session:
                config_obj = await MamIndexer.get_configurations(
                    SessionContainer(session=session, client_session=client_session)
                )
                valued = create_valued_configuration(config_obj, session)
                mam_config = ValuedMamConfigurations(
                    mam_session_id=str(getattr(valued, "mam_session_id") or "")
                )
                mam_result = await fetch_mam_book_details(
                    container=SessionContainer(
                        session=session, client_session=client_session
                    ),
                    configurations=mam_config,
                    mam_id=book_request.mam_id,
                )

            if mam_result and not book.series_index:
                mam_series, mam_index = _normalize_series(mam_result.series)
                if mam_index:
                    book.series_index = mam_index
                if not book.series and mam_series:
                    book.series = mam_series
                session.add(book)
                session.commit()
        except Exception:
            pass

    # Copy and Rename
    padding = 3 if len(audio_files_to_process) >= 100 else 2
    total_files = len(audio_files_to_process)
    for idx, s_path in enumerate(audio_files_to_process, 1):
        ext = os.path.splitext(s_path)[1].lower()
        part_str = f"Part {idx:0{padding}d}" if total_files > 1 else ""

        new_filename = generate_audiobook_filename(book, file_pattern, part_str, ext)

        d_path = os.path.join(dest_path, new_filename)
        smart_copy(s_path, d_path, use_hardlinks, delete_source)

        # Update progress during copying (0.90 to 0.92)
        book_request.download_progress = 0.90 + (idx / total_files * 0.02)
        session.add(book_request)
        session.commit()

    book.downloaded = True
    session.add(book)
    session.commit()

    book_request.processing_status = "generating_metadata"
    book_request.download_progress = 0.95
    log_request_event(
        session,
        book_request.asin,
        book_request.user_username,
        "Generating metadata files.",
        commit=False,
    )
    session.add(book_request)
    session.commit()

    await generate_abs_metadata(book, dest_path, mam_result)
    await generate_opf_metadata(session, book, dest_path, mam_result)

    if book.cover_image:
        book_request.processing_status = "saving_cover"
        book_request.download_progress = 0.98
        log_request_event(
            session,
            book_request.asin,
            book_request.user_username,
            "Saving cover image.",
            commit=False,
        )
        session.add(book_request)
        session.commit()
        try:
            import aiohttp

            async with aiohttp.ClientSession() as client_session:
                async with client_session.get(book.cover_image) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        cover_ext = (
                            os.path.splitext(book.cover_image.split("?")[0])[1]
                            or ".jpg"
                        )
                        cover_path = os.path.join(dest_path, f"cover{cover_ext}")
                        with open(cover_path, "wb") as f:
                            f.write(content)
        except Exception:
            pass

    book_request.processing_status = "completed"
    book_request.download_progress = 1.0
    log_request_event(
        session,
        book_request.asin,
        book_request.user_username,
        "Import completed.",
        commit=False,
    )
    session.add(book_request)
    session.commit()


async def reorganize_existing_book(
    session: Session, book: Audiobook, current_path: Optional[str] = None
):
    """
    Finds a book on disk and re-organizes/re-names its files according to current patterns.
    """
    from app.internal.library.scanner import LibraryScanner

    lib_root = media_management_config.get_library_path(session)
    if not lib_root:
        return

    source_path = current_path or LibraryScanner.find_book_path_by_asin(
        lib_root, book.asin
    )

    if not source_path:
        return

    file_pattern = media_management_config.get_file_pattern(session)
    author = book.authors[0] if book.authors else "Unknown"
    # series = book.series[0] if book.series else None
    year = book.release_date.year if book.release_date else "Unknown"

    folder_rel_path = get_book_folder_path(session, book)
    if not folder_rel_path:
        folder_rel_path = f"{author}/{book.title} ({year})"

    dest_path = os.path.join(lib_root, folder_rel_path)

    if os.path.abspath(source_path) != os.path.abspath(dest_path):
        logger.info(
            "Processor: Moving book folder",
            title=book.title,
            old=source_path,
            new=dest_path,
        )
    else:
        logger.info(
            "Processor: Renaming files within folder", title=book.title, path=dest_path
        )

    audio_files = []
    for root, _dirs, files in os.walk(source_path):
        for f in files:
            if any(
                f.lower().endswith(ext)
                for ext in [
                    ".m4b",
                    ".mp3",
                    ".m4a",
                    ".flac",
                    ".wav",
                    ".ogg",
                    ".opus",
                    ".aac",
                    ".wma",
                ]
            ):
                audio_files.append(os.path.join(root, f))
    audio_files = natural_sort(audio_files) or audio_files
    if not audio_files:
        return

    os.makedirs(dest_path, exist_ok=True)
    new_audio_paths = []
    padding = 3 if len(audio_files) >= 100 else 2
    for idx, s_path in enumerate(audio_files, 1):
        ext = os.path.splitext(s_path)[1].lower()
        part_str = f"Part {idx:0{padding}d}" if len(audio_files) > 1 else ""
        
        new_filename = generate_audiobook_filename(book, file_pattern, part_str, ext)
        
        d_path = os.path.join(dest_path, new_filename)
        if os.path.abspath(s_path) != os.path.abspath(d_path):
            shutil.move(s_path, d_path)
        new_audio_paths.append(d_path)

    moved = set(new_audio_paths)
    for root, dirs, files in os.walk(source_path):
        for f in files:
            s_path = os.path.join(root, f)
            if s_path in moved:
                continue
            rel_dir = os.path.relpath(root, source_path)
            d_dir = os.path.join(dest_path, rel_dir) if rel_dir != "." else dest_path
            os.makedirs(d_dir, exist_ok=True)
            d_path = os.path.join(d_dir, f)
            if os.path.abspath(s_path) != os.path.abspath(d_path):
                shutil.move(s_path, d_path)
            moved.add(s_path)

    # Clean up empty directories left behind
    for root, dirs, files in list(os.walk(source_path, topdown=False)):
        if os.path.abspath(root) == os.path.abspath(dest_path):
            continue
        if not dirs and not files:
            try:
                os.rmdir(root)
            except Exception:
                pass

    # Prune empty parent folders (author/series) up to library root
    def _prune_empty_parents(path: str, stop_at: str):
        path = os.path.abspath(path)
        stop_at = os.path.abspath(stop_at)
        while os.path.commonpath([path, stop_at]) == stop_at and path != stop_at:
            try:
                if os.path.isdir(path) and not os.listdir(path):
                    os.rmdir(path)
                else:
                    break
            except Exception:
                break
            path = os.path.dirname(path)

    if os.path.abspath(source_path) != os.path.abspath(dest_path):
        _prune_empty_parents(source_path, lib_root)

    await generate_abs_metadata(book, dest_path)
    await generate_opf_metadata(session, book, dest_path)
