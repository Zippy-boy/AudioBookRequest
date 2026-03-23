from typing import Annotated

from fastapi import APIRouter, Depends, Security
from sqlmodel import Session

from app.internal.auth.authentication import APIKeyAuth, DetailedUser
from app.internal.db_queries import WishlistCounts, get_wishlist_counts
from app.util.db import get_session

router = APIRouter(prefix="/stats")


@router.get("", response_model=WishlistCounts)
def get_stats(
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[DetailedUser, Security(APIKeyAuth())],
):
    counts = get_wishlist_counts(session, user if not user.is_admin() else None)
    return counts
