from typing import Literal, Optional
from urllib.parse import urlparse
from sqlmodel import Session
from app.util.cache import StringConfigCache

DownloadClientConfigKey = Literal[
    "qbit_host",
    "qbit_port",
    "qbit_user",
    "qbit_pass",
    "qbit_category",
    "qbit_save_path",
    "qbit_enabled",
    "qbit_complete_action",
]


class DownloadClientConfig(StringConfigCache[DownloadClientConfigKey]):
    def get_qbit_base_url(self, session: Session) -> str:
        host = self.get_qbit_host(session)
        port = self.get_qbit_port(session)
        if not host:
            return ""
        if "://" in host:
            parsed = urlparse(host)
            scheme = parsed.scheme or "http"
            base_host = parsed.hostname or host
            return f"{scheme}://{base_host}:{port}".rstrip("/")
        return f"http://{host}:{port}".rstrip("/")

    def set_qbit_base_url(self, session: Session, base_url: str):
        value = base_url.strip()
        if not value:
            self.delete(session, "qbit_host")
            return
        if "://" not in value:
            self.set_qbit_host(session, value.rstrip("/"))
            return
        parsed = urlparse(value)
        if parsed.scheme and parsed.hostname:
            if parsed.port is not None:
                self.set_qbit_port(session, parsed.port)
            host = f"{parsed.scheme}://{parsed.hostname}"
            self.set_qbit_host(session, host.rstrip("/"))
            return
        self.set_qbit_host(session, value.rstrip("/"))

    def get_qbit_host(self, session: Session) -> Optional[str]:
        return self.get(session, "qbit_host")

    def set_qbit_host(self, session: Session, host: str):
        self.set(session, "qbit_host", host)

    def get_qbit_port(self, session: Session) -> int:
        return self.get_int(session, "qbit_port", 8080)

    def set_qbit_port(self, session: Session, port: int):
        self.set_int(session, "qbit_port", port)

    def get_qbit_user(self, session: Session) -> Optional[str]:
        return self.get(session, "qbit_user")

    def set_qbit_user(self, session: Session, user: str):
        self.set(session, "qbit_user", user)

    def get_qbit_pass(self, session: Session) -> Optional[str]:
        return self.get(session, "qbit_pass")

    def set_qbit_pass(self, session: Session, password: str):
        self.set(session, "qbit_pass", password)

    def get_qbit_category(self, session: Session) -> str:
        return self.get(session, "qbit_category", "audiobooks")

    def set_qbit_category(self, session: Session, category: str):
        self.set(session, "qbit_category", category)

    def get_qbit_save_path(self, session: Session) -> Optional[str]:
        return self.get(session, "qbit_save_path")

    def set_qbit_save_path(self, session: Session, path: str):
        self.set(session, "qbit_save_path", path)

    def get_qbit_enabled(self, session: Session) -> bool:
        return bool(self.get_bool(session, "qbit_enabled") or False)

    def set_qbit_enabled(self, session: Session, enabled: bool):
        self.set_bool(session, "qbit_enabled", enabled)

    def get_qbit_complete_action(self, session: Session) -> str:
        return self.get(session, "qbit_complete_action", "copy") or "copy"

    def set_qbit_complete_action(self, session: Session, action: str):
        self.set(session, "qbit_complete_action", action)

download_client_config = DownloadClientConfig()
