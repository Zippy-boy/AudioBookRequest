import os
from typing import Annotated, List
from fastapi import APIRouter, Depends, HTTPException, Security
from pydantic import BaseModel
from app.internal.auth.authentication import APIKeyAuth, DetailedUser
from app.internal.models import GroupEnum

router = APIRouter(prefix="/system", tags=["System"])

class BrowseRequest(BaseModel):
    path: str

class BrowseResponse(BaseModel):
    current_path: str
    parent_path: str | None
    directories: List[str]

@router.post("/browse", response_model=BrowseResponse)
def browse_directories(
    body: BrowseRequest,
    _: Annotated[DetailedUser, Security(APIKeyAuth(GroupEnum.admin))],
):
    path = body.path or "/"
    if not os.path.exists(path):
        # Try to fallback to root or a reasonable default if path doesn't exist
        path = "/"
    
    if not os.path.isdir(path):
        raise HTTPException(status_code=400, detail="Path is not a directory")

    try:
        directories = []
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_dir():
                    directories.append(entry.name)
        
        directories.sort()
        
        parent_path = os.path.dirname(os.path.abspath(path))
        if parent_path == path: # We are at root
            parent_path = None

        return BrowseResponse(
            current_path=os.path.abspath(path),
            parent_path=parent_path,
            directories=directories
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
