from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.internal.env_settings import Settings
from app.internal.processing.monitor import start_monitor
from app.internal.system_user import ensure_root_admin, get_system_user
from app.internal.ui import (
    SpaStaticFiles,
    UI_BUILD_DIR_CANDIDATES,
    ensure_ui_api_key,
    get_ui_build_dir,
)
from app.routers import api
from app.util.db import get_session
from app.util.log import logger

settings = Settings()

docs_url = "/docs" if settings.app.openapi_enabled else None
openapi_url = "/openapi.json" if settings.app.openapi_enabled else None

app = FastAPI(
    title="Narrarr",
    version=settings.app.version,
    description="API for Narrarr (headless)",
    docs_url=docs_url,
    openapi_url=openapi_url,
    redoc_url=None,
)

if settings.app.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.app.cors_origins,
        allow_credentials=settings.app.cors_allow_credentials,
        allow_methods=settings.app.cors_allow_methods or ["*"],
        allow_headers=settings.app.cors_allow_headers or ["*"],
    )


@app.on_event("startup")
async def startup_event():
    # Ensure system user exists for internal operations
    with next(get_session()) as session:
        get_system_user(session)
        ensure_root_admin(
            session,
            username=settings.app.init_root_username,
            password=settings.app.init_root_password,
        )
    await start_monitor()


app.include_router(api.router)


@app.get("/ui/config.json")
async def ui_config(request: Request):
    """Provide UI auto-configuration for the bundled SPA."""
    base = settings.app.base_url.rstrip("/") or str(request.base_url).rstrip("/")
    api_base = f"{base}/api"
    return JSONResponse(
        {
            "apiBaseUrl": api_base,
        }
    )


ui_build_dir = get_ui_build_dir()
if ui_build_dir:
    logger.info("Mounting bundled web UI", path=str(ui_build_dir))
    app.mount(
        "/ui",
        SpaStaticFiles(directory=str(ui_build_dir), html=True),
        name="narrarr-ui",
    )
else:
    logger.warning(
        "Bundled web UI not mounted; build directory missing",
        expected_paths=[str(path) for path in UI_BUILD_DIR_CANDIDATES],
    )
