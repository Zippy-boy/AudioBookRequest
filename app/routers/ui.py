from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse

router = APIRouter(prefix="/ui")

UI_DIST = Path("ui/dist")
UI_ROOT = UI_DIST.resolve()


@router.get("/config.json")
def read_ui_config():
    return JSONResponse({"apiBaseUrl": "/api", "apiKey": ""})


@router.get("")
@router.get("/{path:path}")
def read_ui(path: str = ""):
    if path:
        target = (UI_DIST / path).resolve()
        if UI_ROOT in target.parents and target.is_file():
            return FileResponse(target)
    return FileResponse(UI_DIST / "index.html")
