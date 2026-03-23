from sqlmodel import Session

from app.internal.models import GroupEnum, User
from app.internal.auth.authentication import create_user, raise_for_invalid_password
from app.util.log import logger


def get_system_user(session: Session) -> User:
    """
    Ensure a built-in system user exists for internal operations when auth is disabled.
    """
    user = session.get(User, "system")
    if user:
        return user

    user = User(username="system", password="", group=GroupEnum.admin, root=True)
    session.add(user)
    session.commit()
    return user


def ensure_root_admin(session: Session, username: str, password: str) -> bool:
    """
    Create an initial root admin user when credentials are provided via settings.
    Returns True when a user was created.
    """
    if not username or not password:
        return False

    existing = session.get(User, username)
    if existing:
        return False

    try:
        raise_for_invalid_password(session, password, ignore_confirm=True)
    except Exception as exc:  # HTTPException or validation error
        logger.error("Failed to create root user from settings", error=str(exc))
        return False

    user = create_user(username=username, password=password, group=GroupEnum.admin, root=True)
    session.add(user)
    session.commit()
    logger.info("Created root admin user from settings", username=username)
    return True
