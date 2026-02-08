from fastapi import APIRouter

from app.routers.api.settings.audiobookshelf import router as audiobookshelf_router
from app.routers.api.settings.download import router as download_router
from app.routers.api.settings.notifications import router as notifications_router
from app.routers.api.settings.prowlarr import router as prowlarr_router

router = APIRouter(prefix="/settings", tags=["Settings"])

router.include_router(audiobookshelf_router)
router.include_router(download_router)
router.include_router(notifications_router)
router.include_router(prowlarr_router)
