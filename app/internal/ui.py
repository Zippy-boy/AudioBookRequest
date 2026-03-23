from pathlib import Path

from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.util.log import logger

UI_BUILD_DIR_CANDIDATES = [
    Path(__file__).resolve().parent.parent / "static" / "ui",
    Path(__file__).resolve().parent.parent.parent / "NarrarrUI" / "build" / "web",
]


def get_ui_build_dir() -> Path | None:
    """
    Resolve the built SPA directory inside the backend image.

    We support the current Docker image layout as well as the newer
    app/static/ui location so the backend can run against either packaging
    arrangement without code changes.
    """
    for candidate in UI_BUILD_DIR_CANDIDATES:
        if (candidate / "index.html").exists():
            return candidate
    return None


class SpaStaticFiles(StaticFiles):
    """
    StaticFiles handler with SPA fallback behavior.

    Unknown GET/HEAD routes under the mount point fall back to index.html so
    client-side routing works without shadowing /api routes.
    """

    async def get_response(self, path: str, scope):  # type: ignore[override]
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404 or scope.get("method") not in {"GET", "HEAD"}:
                raise
            return await super().get_response("index.html", scope)
