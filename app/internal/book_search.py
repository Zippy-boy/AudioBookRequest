import asyncio
import time
import json
import re
from datetime import datetime
from typing import Any, Literal, TypedDict, Union, cast
from urllib.parse import urlencode

from aiohttp import ClientSession
from pydantic import BaseModel
from sqlalchemy import CursorResult, delete
from sqlalchemy.exc import InvalidRequestError
from sqlmodel import Session, col, not_, select

from app.internal.audiobookshelf.client import abs_mark_downloaded_flags
from app.internal.audiobookshelf.config import abs_config
from app.internal.env_settings import Settings
from app.internal.models import Audiobook, AudiobookRequest, LibraryImportItem, User
from app.util.connection import USER_AGENT
from app.util.log import logger

REFETCH_TTL = 60 * 60 * 24 * 7  # 1 week

audible_region_type = Literal[
    "us",
    "ca",
    "uk",
    "au",
    "fr",
    "de",
    "jp",
    "it",
    "in",
    "es",
    "br",
]
audible_regions: dict[audible_region_type, str] = {
    "us": ".com",
    "ca": ".ca",
    "uk": ".co.uk",
    "au": ".com.au",
    "fr": ".fr",
    "de": ".de",
    "jp": ".co.jp",
    "it": ".it",
    "in": ".in",
    "es": ".es",
    "br": ".com.br",
}

_SERIES_INDEX_RE = re.compile(
    r"(?:#\s*|(?:Book|Bk\.?|Vol\.?|Volume)\s*)(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _normalize_series(series_list: list[str] | None) -> tuple[list[str], str | None]:
    if not series_list:
        return [], None
    raw = next((s for s in series_list if s and s.strip()), "")
    if not raw:
        return [], None
    name = raw.strip()
    index: str | None = None
    if " #" in name:
        base, idx = name.split(" #", 1)
        name = base.strip()
        index = idx.strip() or None
    else:
        match = _SERIES_INDEX_RE.search(name)
        if match:
            index = match.group(1)
            name = name[: match.start()].strip(" -,:")
    return ([name] if name else []), index


def clear_old_book_caches(session: Session):
    """Deletes cached audiobooks that aren't downloaded or referenced by any request"""
    deletable_asins = select(col(Audiobook.asin)).where(
        not_(Audiobook.downloaded),
        not_(
            select(AudiobookRequest.asin)
            .where(AudiobookRequest.asin == Audiobook.asin)
            .exists()
        ),
        not_(
            select(LibraryImportItem.match_asin)
            .where(LibraryImportItem.match_asin == Audiobook.asin)
            .exists()
        ),
    )

    result = cast(
        CursorResult[Audiobook],
        session.execute(
            delete(Audiobook).where(col(Audiobook.asin).in_(deletable_asins))
        ),
    )
    session.commit()
    logger.debug("Cleared cached audiobooks on startup", rowcount=result.rowcount)


def get_region_from_settings(user: User | None = None) -> audible_region_type:
    if user and user.extra_data:
        try:
            data = json.loads(user.extra_data)
            if isinstance(data, dict):
                region = data.get("default_region")
                if region in audible_regions:
                    return cast(audible_region_type, region)
        except Exception:
            pass

    region = Settings().app.default_region
    if region not in audible_regions:
        return "us"
    return region


class _AudnexusResponse(BaseModel):
    class _Author(TypedDict):
        name: str

    asin: str
    title: str
    subtitle: str | None = None
    authors: list[_Author]
    narrators: list[_Author]
    series: list[_Author] | None = None
    genres: list[Union[str, dict[str, Any]]] | None = None
    publisher: str | None = None
    description: str | None = None
    language: str | None = None
    image: str | None
    releaseDate: str
    runtimeLengthMin: int


async def _get_audnexus_book(
    client_session: ClientSession,
    asin: str,
    region: audible_region_type,
) -> Audiobook | None:
    """
    https://audnex.us/#tag/Books/operation/getBookById
    """
    logger.debug("Fetching book from Audnexus", asin=asin, region=region)
    try:
        async with client_session.get(
            f"https://api.audnex.us/books/{asin}?region={region}",
            headers={"Client-Agent": "audiobookrequest", "User-Agent": USER_AGENT},
        ) as response:
            if not response.ok:
                logger.warning(
                    "Failed to fetch book from Audnexus",
                    asin=asin,
                    status=response.status,
                    reason=response.reason,
                )
                return None
            audnexus_response = _AudnexusResponse.model_validate(await response.json())
    except Exception as e:
        logger.error("Exception while fetching book from Audnexus", asin=asin, error=e)
        return None

    # Safely parse genres which can be strings or dictionaries
    parsed_genres = []
    if audnexus_response.genres:
        for g in audnexus_response.genres:
            if isinstance(g, str):
                parsed_genres.append(g)
            elif isinstance(g, dict):
                # Try common keys for genre names/labels
                name = g.get("name") or g.get("label") or g.get("title")
                if name:
                    parsed_genres.append(name)

    series_list, series_index = _normalize_series(
        [s["name"] for s in audnexus_response.series]
        if audnexus_response.series
        else []
    )

    return Audiobook(
        asin=audnexus_response.asin,
        title=audnexus_response.title,
        subtitle=audnexus_response.subtitle,
        authors=[author["name"] for author in audnexus_response.authors],
        narrators=[narrator["name"] for narrator in audnexus_response.narrators],
        series=series_list,
        series_index=series_index,
        genres=parsed_genres,
        publisher=audnexus_response.publisher,
        description=audnexus_response.description,
        language=audnexus_response.language,
        cover_image=audnexus_response.image,
        release_date=datetime.fromisoformat(audnexus_response.releaseDate),
        runtime_length_min=audnexus_response.runtimeLengthMin,
    )


class _AudimetaResponse(BaseModel):
    class _Author(TypedDict):
        name: str

    class _Series(TypedDict):
        name: str

    asin: str
    title: str
    subtitle: str | None = None
    authors: list[_Author]
    narrators: list[_Author]
    series: list[_Series] | None = None
    genres: list[Union[str, dict[str, Any]]] | None = None
    publisher: str | None = None
    description: str | None = None
    language: str | None = None
    imageUrl: str | None
    releaseDate: str
    lengthMinutes: int | None


async def _get_audimeta_book(
    client_session: ClientSession,
    asin: str,
    region: audible_region_type,
) -> Audiobook | None:
    """
    https://audimeta.de/api-docs/#/book/get_book__asin_
    """
    logger.debug("Fetching book from Audimeta", asin=asin, region=region)
    try:
        async with client_session.get(
            f"https://audimeta.de/book/{asin}?region={region}",
            headers={"Client-Agent": "audiobookrequest", "User-Agent": USER_AGENT},
        ) as response:
            if not response.ok:
                logger.warning(
                    "Failed to fetch book from Audimeta",
                    asin=asin,
                    status=response.status,
                    reason=response.reason,
                )
                return None
            audimeta_response = _AudimetaResponse.model_validate(await response.json())
    except Exception as e:
        logger.error("Exception while fetching book from Audimeta", asin=asin, error=e)
        return None

    # Safely parse genres which can be strings or dictionaries
    parsed_genres = []
    if audimeta_response.genres:
        for g in audimeta_response.genres:
            if isinstance(g, str):
                parsed_genres.append(g)
            elif isinstance(g, dict):
                # Try common keys for genre names/labels
                name = g.get("name") or g.get("label") or g.get("title")
                if name:
                    parsed_genres.append(name)

    series_list, series_index = _normalize_series(
        [s["name"] for s in audimeta_response.series]
        if audimeta_response.series
        else []
    )

    return Audiobook(
        asin=audimeta_response.asin,
        title=audimeta_response.title,
        subtitle=audimeta_response.subtitle,
        authors=[author["name"] for author in audimeta_response.authors],
        narrators=[narrator["name"] for narrator in audimeta_response.narrators],
        series=series_list,
        series_index=series_index,
        genres=parsed_genres,
        publisher=audimeta_response.publisher,
        description=audimeta_response.description,
        language=audimeta_response.language,
        cover_image=audimeta_response.imageUrl,
        release_date=datetime.fromisoformat(audimeta_response.releaseDate),
        runtime_length_min=audimeta_response.lengthMinutes or 0,
    )


class _AudibleProductResponse(BaseModel):
    class _Author(BaseModel):
        name: str

    class _Narrator(BaseModel):
        name: str

    class _Series(BaseModel):
        name: str
        sequence: str | None = None

    asin: str
    title: str
    subtitle: str | None = None
    authors: list[_Author] | None = None
    narrators: list[_Narrator] | None = None
    series: list[_Series] | None = None
    publisher_name: str | None = None
    product_description: str | None = None
    publication_date: str | None = None
    runtime_length_min: int | None = None
    language: str | None = None
    product_images: dict[str, str] | None = None
    categories: list[dict[str, Any]] | None = None
    sku: str | None = None


class _AudibleProductContainer(BaseModel):
    product: _AudibleProductResponse


def _best_image(images: dict[str, str] | None) -> str | None:
    if not images:
        return None
    for res in ("500", "558", "360", "315", "252", "210", "180", "150", "120", "90"):
        url = images.get(res)
        if url:
            return url
    return next(iter(images.values()), None)


async def _get_audible_book(
    client_session: ClientSession,
    asin: str,
    region: audible_region_type,
) -> Audiobook | None:
    """
    Fallback: fetch book details directly from the Audible catalog API.
    https://audible.readthedocs.io/en/latest/misc/external_api.html
    """
    base_url = f"https://api.audible{audible_regions[region]}/1.0/catalog/products/{asin}"
    url = base_url
    logger.debug("Fetching book from Audible API", asin=asin, region=region)
    try:
        async with client_session.get(
            url, headers={"User-Agent": USER_AGENT}
        ) as response:
            if not response.ok:
                logger.warning(
                    "Failed to fetch book from Audible API",
                    asin=asin,
                    status=response.status,
                    reason=response.reason,
                )
                return None
            container = _AudibleProductContainer.model_validate(await response.json())
    except Exception as e:
        logger.error("Exception while fetching book from Audible API", asin=asin, error=e)
        return None

    p = container.product
    parsed_genres = []
    if p.categories:
        for cat in p.categories:
            name = cat.get("name") or cat.get("ladder", [{}])[-1].get("name", "")
            if name:
                parsed_genres.append(name)

    series_list, series_index = _normalize_series(
        [s.name for s in p.series] if p.series else []
    )

    release_date = None
    if p.publication_date:
        try:
            release_date = datetime.fromisoformat(p.publication_date.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            release_date = datetime.now()

    cover_url = _best_image(p.product_images)

    return Audiobook(
        asin=p.asin,
        title=p.title,
        subtitle=p.subtitle,
        authors=[a.name for a in p.authors] if p.authors else [],
        narrators=[n.name for n in p.narrators] if p.narrators else [],
        series=series_list,
        series_index=series_index,
        genres=parsed_genres,
        publisher=p.publisher_name,
        description=p.product_description,
        language=p.language,
        cover_image=cover_url,
        release_date=release_date or datetime.now(),
        runtime_length_min=p.runtime_length_min or 0,
    )


class _ITunesResult(BaseModel):
    collectionName: str | None = None
    artistName: str | None = None
    collectionDescription: str | None = None
    collectionViewUrl: str | None = None
    artworkUrl100: str | None = None
    releaseDate: str | None = None
    primaryGenreName: str | None = None
    genres: list[str] | None = None
    description: str | None = None
    trackCount: int | None = None


class _ITunesResponse(BaseModel):
    resultCount: int
    results: list[_ITunesResult]


async def _get_itunes_book(
    client_session: ClientSession,
    asin: str,
) -> Audiobook | None:
    """
    Fallback: search Apple iTunes for an audiobook by ASIN.
    https://developer.apple.com/library/archive/documentation/AudioVideo/Conceptual/iTuneSearchAPI/
    """
    url = f"https://itunes.apple.com/search?term={asin}&media=audiobook&entity=audiobook&limit=1"
    logger.debug("Fetching book from Apple iTunes", asin=asin)
    try:
        async with client_session.get(
            url, headers={"User-Agent": USER_AGENT}
        ) as response:
            if not response.ok:
                logger.warning(
                    "Failed to fetch book from Apple iTunes",
                    asin=asin,
                    status=response.status,
                    reason=response.reason,
                )
                return None
            itunes_response = _ITunesResponse.model_validate(await response.json())
    except Exception as e:
        logger.error("Exception while fetching book from Apple iTunes", asin=asin, error=e)
        return None

    if itunes_response.resultCount == 0 or not itunes_response.results:
        logger.debug("Apple iTunes returned no results", asin=asin)
        return None

    r = itunes_response.results[0]
    parsed_genres = []
    if r.genres:
        parsed_genres.extend(r.genres)
    if r.primaryGenreName and r.primaryGenreName not in parsed_genres:
        parsed_genres.append(r.primaryGenreName)

    release_date = None
    if r.releaseDate:
        try:
            release_date = datetime.fromisoformat(r.releaseDate.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass

    return Audiobook(
        asin=asin,
        title=r.collectionName or "Unknown",
        subtitle=None,
        authors=[r.artistName] if r.artistName else [],
        narrators=[],
        series=[],
        series_index=None,
        genres=parsed_genres,
        publisher=None,
        description=r.description or r.collectionDescription,
        language=None,
        cover_image=r.artworkUrl100,
        release_date=release_date or datetime.now(),
        runtime_length_min=0,
    )


async def get_book_by_asin(
    client_session: ClientSession,
    asin: str,
    audible_region: audible_region_type | None = None,
) -> Audiobook | None:
    if audible_region is None:
        audible_region = get_region_from_settings()
    book = await _get_audimeta_book(client_session, asin, audible_region)
    if book:
        return book
    logger.debug(
        "Audimeta did not have the book, trying Audnexus",
        asin=asin,
        region=audible_region,
    )
    book = await _get_audnexus_book(client_session, asin, audible_region)
    if book:
        return book
    logger.debug(
        "Audnexus did not have the book, trying Audible API",
        asin=asin,
        region=audible_region,
    )
    book = await _get_audible_book(client_session, asin, audible_region)
    if book:
        return book
    logger.debug(
        "Audible API did not have the book, trying Apple iTunes",
        asin=asin,
        region=audible_region,
    )
    book = await _get_itunes_book(client_session, asin)
    if book:
        return book
    logger.warning(
        "Did not find the book on Audimeta, Audnexus, Audible API, or Apple iTunes",
        asin=asin,
        region=audible_region,
    )


class CacheQuery(BaseModel, frozen=True):
    query: str
    num_results: int
    page: int
    audible_region: audible_region_type


class CacheResult[T](BaseModel, frozen=True):
    value: T
    timestamp: float


# simple caching of search results to avoid having to fetch from audible so frequently
search_cache: dict[CacheQuery, CacheResult[list[Audiobook]]] = {}
search_suggestions_cache: dict[str, CacheResult[list[str]]] = {}


class _AudibleSuggestionsResponse(BaseModel):
    """Used for type-checking audible search suggestions response"""

    class _Items(BaseModel):
        class _Item(BaseModel):
            class _Model(BaseModel):
                class _Metadata(BaseModel):
                    class _Title(BaseModel):
                        value: str

                    title: _Title

                class _TitleGroup(BaseModel):
                    class _Title(BaseModel):
                        value: str

                    title: _Title

                product_metadata: _Metadata | None = None
                title_group: _TitleGroup | None = None

                @property
                def title(self) -> str | None:
                    if self.product_metadata and self.product_metadata.title:
                        return self.product_metadata.title.value
                    if self.title_group and self.title_group.title:
                        return self.title_group.title.value
                    return None

            model: _Model

        items: list[_Item]

    model: _Items


async def get_search_suggestions(
    client_session: ClientSession,
    query: str,
    audible_region: audible_region_type | None = None,
) -> list[str]:
    if audible_region is None:
        audible_region = get_region_from_settings()
    cache_result = search_suggestions_cache.get(query)
    if cache_result and time.time() - cache_result.timestamp < REFETCH_TTL:
        return cache_result.value

    params = {
        "key_strokes": query,
        "site_variant": "desktop",
    }
    base_url = (
        f"https://api.audible{audible_regions[audible_region]}/1.0/searchsuggestions?"
    )
    url = base_url + urlencode(params)

    try:
        async with client_session.get(
            url, headers={"User-Agent": USER_AGENT}
        ) as response:
            response.raise_for_status()
            suggestions = _AudibleSuggestionsResponse.model_validate(
                await response.json()
            )
    except Exception as e:
        logger.error(
            "Exception while fetching search suggestions from Audible",
            query=query,
            region=audible_region,
            error=e,
        )
        return []

    titles = [item.model.title for item in suggestions.model.items if item.model.title]
    search_suggestions_cache[query] = CacheResult(
        value=titles,
        timestamp=time.time(),
    )

    return titles


class _AudibleSearchResponse(BaseModel):
    class _AsinObj(BaseModel):
        asin: str

    products: list[_AsinObj]


async def list_audible_books(
    session: Session,
    client_session: ClientSession,
    query: str,
    num_results: int = 20,
    page: int = 0,
    audible_region: audible_region_type | None = None,
) -> list[Audiobook]:
    """
    https://audible.readthedocs.io/en/latest/misc/external_api.html#get--1.0-catalog-products

    We first use the audible search API to get a list of matching ASINs. Using these ASINs we check our database
    if we have any of the books already to save on the amount of requests we have to do.
    Any books we don't already have locally, we fetch all the details from audnexus.
    """
    if audible_region is None:
        audible_region = get_region_from_settings()
    cache_key = CacheQuery(
        query=query,
        num_results=num_results,
        page=page,
        audible_region=audible_region,
    )
    cache_result = search_cache.get(cache_key)

    if cache_result and time.time() - cache_result.timestamp < REFETCH_TTL:
        try:
            for book in cache_result.value:
                # add back books to the session so we can access their attributes
                session.add(book)
                session.refresh(book)
            logger.debug(
                "Using cached search result", query=query, region=audible_region
            )
            return cache_result.value
        except InvalidRequestError:
            logger.debug(
                "Cached search result contained deleted book, refetching",
                query=query,
                region=audible_region,
            )

    params = {
        "num_results": num_results,
        "products_sort_by": "Relevance",
        "keywords": query,
        "page": page,
    }
    base_url = (
        f"https://api.audible{audible_regions[audible_region]}/1.0/catalog/products?"
    )
    url = base_url + urlencode(params)

    try:
        async with client_session.get(
            url, headers={"User-Agent": USER_AGENT}
        ) as response:
            response.raise_for_status()
            audible_response = _AudibleSearchResponse.model_validate(
                await response.json()
            )
    except Exception as e:
        logger.error(
            "Exception while fetching search results from Audible",
            query=query,
            region=audible_region,
            error=e,
        )
        return []

    # do not fetch book results we already have locally
    asins = set(asin_obj.asin for asin_obj in audible_response.products)
    books = get_existing_books(session, asins)
    missing_asins = {a for a in asins if a not in books.keys()}
    logger.debug(
        "Search results fetched",
        query=query,
        region=audible_region,
        total_results=len(audible_response.products),
        cached_results=len(books),
        missing_results=len(missing_asins),
    )

    # book ASINs we do not have => fetch and store
    coros = [
        get_book_by_asin(client_session, asin, audible_region) for asin in missing_asins
    ]
    new_books_fetched = await asyncio.gather(*coros)
    new_books_fetched = [b for b in new_books_fetched if b]

    # store_new_books now returns the merged, session-attached instances
    new_books = store_new_books(session, new_books_fetched)

    for b in new_books:
        books[b.asin] = b

    ordered: list[Audiobook] = []
    for asin_obj in audible_response.products:
        book = books.get(asin_obj.asin)
        if book:
            ordered.append(book)

    try:
        if abs_config.is_valid(session):
            await abs_mark_downloaded_flags(session, client_session, ordered)
    except Exception as e:
        logger.error("Failed to mark ABS downloaded flags on search results", error=e)

    search_cache[cache_key] = CacheResult(
        value=ordered,
        timestamp=time.time(),
    )

    # clean up cache slightly
    for k in list(search_cache.keys()):
        if time.time() - search_cache[k].timestamp > REFETCH_TTL:
            try:
                del search_cache[k]
            except KeyError:  # ignore in race conditions
                pass

    return ordered


def get_existing_books(session: Session, asins: set[str]) -> dict[str, Audiobook]:
    books = session.exec(select(Audiobook).where(col(Audiobook.asin).in_(asins))).all()
    ok_books: list[Audiobook] = []
    for b in books:
        # If series is missing, we consider the metadata "incomplete" and force a re-fetch
        # even if it's within the TTL. This ensures books are grouped correctly.
        has_series = b.series and len(b.series) > 0
        is_fresh = b.updated_at.timestamp() + REFETCH_TTL > time.time()

        if is_fresh and has_series:
            ok_books.append(b)
        else:
            logger.debug(
                "Book metadata considered stale or incomplete",
                asin=b.asin,
                has_series=bool(has_series),
            )

    return {b.asin: b for b in ok_books}


def store_new_books(session: Session, books: list[Audiobook]) -> list[Audiobook]:
    """
    Stores new search results in the database, merging them if they already exist.
    Returns the merged (session-attached) instances.
    """
    if not books:
        return []

    merged_books = []
    for book in books:
        series_list, series_index = _normalize_series(book.series)
        book.series = series_list
        book.series_index = series_index

        # Check if book exists first to preserve downloaded status
        existing = session.get(Audiobook, book.asin)
        if existing:
            # Update metadata but keep the downloaded flag from the DB
            book.downloaded = existing.downloaded

        # merge() returns the instance that is actually attached to the session
        merged = session.merge(book)
        merged_books.append(merged)

    session.commit()
    logger.info(
        "Stored/Updated search results in BookRequest cache/db", count=len(merged_books)
    )
    return merged_books
