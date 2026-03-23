from sqlmodel import Session

from app.util.cache import StringConfigCache


class SetupState(StringConfigCache[str]):
    KEY = "setup_completed"

    def is_complete(self, session: Session) -> bool:
        return self.get_bool(session, self.KEY) is True

    def mark_complete(self, session: Session) -> None:
        self.set_bool(session, self.KEY, True)

    def reset(self, session: Session) -> None:
        self.set_bool(session, self.KEY, False)


setup_state = SetupState()
