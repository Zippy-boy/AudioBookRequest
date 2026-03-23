import aiohttp

from app.internal.env_settings import Settings

DEFAULT_TIMEOUT_SECONDS = 30


def _build_user_agent() -> str:
    version = Settings().app.version
    return f"Narrarr/{version} (+https://github.com/markbeep/AudioBookRequest)"


async def get_connection():
    timeout = aiohttp.ClientTimeout(DEFAULT_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        yield session


USER_AGENT = _build_user_agent()
