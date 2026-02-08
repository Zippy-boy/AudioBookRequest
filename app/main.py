from fastapi import FastAPI

from app.internal.env_settings import Settings
from app.internal.processing.monitor import start_monitor
from app.routers import api
from app.internal.system_user import get_system_user
from app.util.db import get_session

app = FastAPI(
    title="Narrarr",
    version=Settings().app.version,
    description="API for Narrarr (headless)",
)


@app.on_event("startup")
async def startup_event():
    # Ensure system user exists for internal operations
    with next(get_session()) as session:
        get_system_user(session)
    await start_monitor()


# API router under /api
app.include_router(api.router)
