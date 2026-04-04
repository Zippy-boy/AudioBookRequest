from sqlmodel import Session, select

from app.internal.audiobookshelf.config import abs_config
from app.internal.download_clients.config import download_client_config
from app.internal.media_management.config import media_management_config
from app.internal.models import User
from app.internal.prowlarr.util import prowlarr_config
from app.util.cache import StringConfigCache


class SetupState(StringConfigCache[str]):
    KEY = "setup_completed"

    def is_complete(self, session: Session) -> bool:
        return bool(self.get_bool(session, self.KEY))

    def mark_complete(self, session: Session):
        self.set_bool(session, self.KEY, True)

    def reset(self, session: Session):
        self.set_bool(session, self.KEY, False)

    def is_auth_initialized(self, session: Session) -> bool:
        return session.exec(select(User).limit(1)).first() is not None

    def is_prowlarr_ready(self, session: Session) -> bool:
        return prowlarr_config.is_valid(session)

    def is_download_client_ready(self, session: Session) -> bool:
        return bool(
            download_client_config.get_qbit_base_url(session).strip()
            and (download_client_config.get_qbit_save_path(session) or "").strip()
        )

    def is_media_management_ready(self, session: Session) -> bool:
        return bool(
            (media_management_config.get_library_path(session) or "").strip()
            and media_management_config.get_folder_pattern(session).strip()
            and media_management_config.get_file_pattern(session).strip()
        )

    def is_audiobookshelf_ready(self, session: Session) -> bool:
        return bool(
            (abs_config.get_base_url(session) or "").strip()
            and (abs_config.get_api_token(session) or "").strip()
            and (abs_config.get_library_id(session) or "").strip()
        )

    def is_runtime_complete(self, session: Session) -> bool:
        return (
            self.is_auth_initialized(session)
            and self.is_prowlarr_ready(session)
            and self.is_download_client_ready(session)
            and self.is_media_management_ready(session)
            and self.is_audiobookshelf_ready(session)
        )

    def requires_setup(self, session: Session) -> bool:
        return not self.is_complete(session) or not self.is_runtime_complete(session)


setup_state = SetupState()
