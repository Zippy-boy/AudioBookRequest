from fastapi.responses import RedirectResponse
from starlette.datastructures import URL

from app.internal.env_settings import Settings


class BaseUrlRedirectResponse(RedirectResponse):
    """
    Redirects while preserving the base URL
    """

    @staticmethod
    def _is_relative(url: str | URL) -> bool:
        if isinstance(url, str):
            return url.startswith("/")
        return url.path.startswith("/")

    def __init__(self, url: str | URL, status_code: int = 302) -> None:
        if self._is_relative(url):
            url = f"{Settings().app.base_url.rstrip('/')}{url}"
        super().__init__(
            url=url,
            status_code=status_code,
        )
