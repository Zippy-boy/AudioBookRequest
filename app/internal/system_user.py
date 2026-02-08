from sqlmodel import Session

from app.internal.models import GroupEnum, User


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
