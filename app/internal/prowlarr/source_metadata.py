import asyncio
from typing import Awaitable

from app.internal.indexers.abstract import SessionContainer
from app.internal.indexers.indexer_util import get_indexer_contexts
from app.internal.models import Audiobook, ProwlarrSource
from app.util.log import logger


def _log_exceptions(errors: list[object], message: str) -> None:
    for exc in errors:
        if exc:
            logger.error(message, error=str(exc))


async def edit_source_metadata(
    book: Audiobook,
    sources: list[ProwlarrSource],
    container: SessionContainer,
):
    contexts = await get_indexer_contexts(container)

    coros: list[Awaitable[None]] = [
        context.indexer.setup(book, container, context.valued) for context in contexts
    ]
    exceptions = await asyncio.gather(*coros, return_exceptions=True)
    _log_exceptions(exceptions, "Failed to setup indexer")

    coros = []
    for source in sources:
        for context in contexts:
            if await context.indexer.is_matching_source(source, container):
                coros.append(context.indexer.edit_source_metadata(source, container))
                break

    exceptions = await asyncio.gather(*coros, return_exceptions=True)
    _log_exceptions(exceptions, "Failed to edit source metadata")
