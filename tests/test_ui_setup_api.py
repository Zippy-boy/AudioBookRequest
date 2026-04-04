import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

import app.main as main_module
from app.internal.audiobookshelf.config import abs_config
from app.internal.auth.config import auth_config
from app.internal.download_clients.config import download_client_config
from app.internal.media_management.config import media_management_config
from app.internal.models import Config
from app.internal.prowlarr.prowlarr import Indexer, IndexerResponse
from app.internal.prowlarr.util import prowlarr_config
from app.internal.setup_state import setup_state
from app.util.connection import get_connection
from app.util.db import get_session


class UISetupAPITest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        SQLModel.metadata.create_all(self.engine)
        for cache in (
            abs_config,
            auth_config,
            download_client_config,
            media_management_config,
            prowlarr_config,
            setup_state,
        ):
            cache._cache = {}

        async def override_connection():
            yield object()

        def override_session():
            with Session(self.engine) as session:
                yield session

        main_module.app.dependency_overrides[get_session] = override_session
        main_module.app.dependency_overrides[get_connection] = override_connection
        main_module.start_monitor = AsyncMock()
        self.client = TestClient(main_module.app)

    def tearDown(self):
        self.client.close()
        main_module.app.dependency_overrides.clear()

    def test_ui_config_and_setup_routes_work_before_init(self):
        response = self.client.get("/ui/config.json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["apiBaseUrl"], "/api")

        with patch(
            "app.routers.api.settings.prowlarr.get_indexers",
            new=AsyncMock(return_value=IndexerResponse(state="ok", indexers={})),
        ):
            response = self.client.get("/api/settings/prowlarr")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["base_url"], "")

        response = self.client.get("/api/auth/status")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["initialized"])

        response = self.client.get("/api/setup")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["setup_required"])

        response = self.client.get("/ui/setup")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])

    def test_initialize_login_and_complete_setup_flow(self):
        response = self.client.post(
            "/api/auth/initialize",
            json={
                "login_type": "forms",
                "username": "admin",
                "password": "secret",
                "confirm_password": "secret",
            },
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "secret"},
        )
        self.assertEqual(response.status_code, 200)
        api_key = response.json()["api_key"]
        auth_headers = {"Authorization": f"Bearer {api_key}"}

        response = self.client.patch(
            "/api/settings/download-client",
            json={
                "qbit_base_url": "http://qbittorrent:8080",
                "qbit_user": "user",
                "qbit_pass": "pass",
                "qbit_category": "audiobooks",
                "qbit_save_path": "/downloads",
                "qbit_enabled": True,
                "qbit_complete_action": "copy",
            },
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.patch(
            "/api/settings/media-management",
            json={
                "library_path": "/library",
                "folder_pattern": "{author}/{title}",
                "file_pattern": "{title}",
                "use_series_folders": True,
                "use_hardlinks": False,
                "review_before_import": False,
            },
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.put(
            "/api/settings/prowlarr/base-url",
            json={"base_url": "http://prowlarr:9696"},
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.put(
            "/api/settings/prowlarr/api-key",
            json={"api_key": "prowlarr-key"},
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.put(
            "/api/settings/audiobookshelf/base-url",
            data={"base_url": "http://audiobookshelf:13378"},
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.put(
            "/api/settings/audiobookshelf/api-token",
            data={"api_token": "abs-token"},
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.put(
            "/api/settings/audiobookshelf/library",
            data={"library_id": "library-1"},
            headers=auth_headers,
        )
        self.assertEqual(response.status_code, 204)

        with patch(
            "app.routers.api.settings.prowlarr.get_indexers",
            new=AsyncMock(
                return_value=IndexerResponse(
                    state="ok",
                    indexers={1: Indexer(id=1, name="Main", enable=True, privacy="private")},
                )
            ),
        ):
            response = self.client.get(
                "/api/settings/prowlarr/test-connection",
                headers=auth_headers,
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["indexer_count"], 1)

        response = self.client.post("/api/setup/complete", headers=auth_headers)
        self.assertEqual(response.status_code, 204)

        response = self.client.get("/api/setup/overview")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["setup_complete"])

        with Session(self.engine) as session:
            setup_flag = session.get(Config, "setup_completed")
            self.assertIsNotNone(setup_flag)
            self.assertEqual(setup_flag.value, "1")


if __name__ == "__main__":
    unittest.main()
