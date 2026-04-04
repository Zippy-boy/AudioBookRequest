import os
import tempfile
import unittest
from unittest.mock import patch

from sqlmodel import SQLModel


_TMPDIR = tempfile.TemporaryDirectory()
os.environ["ABR_APP__CONFIG_DIR"] = _TMPDIR.name
os.environ["ABR_DB__SQLITE_PATH"] = "test.db"
os.environ["ABR_APP__DEBUG"] = "true"

from fastapi.testclient import TestClient

from app.internal.audiobookshelf.config import abs_config
from app.internal.auth.config import auth_config
from app.internal.download_clients.config import download_client_config
from app.internal.media_management.config import media_management_config
from app.internal.prowlarr.util import prowlarr_config
from app.internal.setup_state import setup_state
from app.util.db import get_session
from app.util.db import engine

SQLModel.metadata.create_all(engine)

from app.main import app


class SetupApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SQLModel.metadata.create_all(engine)
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        engine.dispose()
        _TMPDIR.cleanup()

    def setUp(self):
        SQLModel.metadata.drop_all(engine)
        SQLModel.metadata.create_all(engine)
        for cache in [
            abs_config,
            auth_config,
            download_client_config,
            media_management_config,
            prowlarr_config,
            setup_state,
        ]:
            cache._cache = {}

    def test_settings_are_accessible_during_setup(self):
        response = self.client.get("/api/settings/prowlarr")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["base_url"], "")

        response = self.client.put(
            "/api/settings/prowlarr/base-url",
            json={"base_url": "http://prowlarr:9696"},
        )
        self.assertEqual(response.status_code, 204)

        response = self.client.get("/api/settings/download-client")
        self.assertEqual(response.status_code, 200)

        response = self.client.patch(
            "/api/settings/download-client",
            json={
                "qbit_base_url": "http://qbittorrent:8080",
                "qbit_user": "admin",
                "qbit_pass": "secret",
                "qbit_category": "audiobooks",
                "qbit_save_path": "/downloads",
                "qbit_enabled": True,
                "qbit_complete_action": "copy",
            },
        )
        self.assertEqual(response.status_code, 204)

    def test_stale_setup_complete_flag_does_not_lock_setup_routes(self):
        setup_state._cache = {}
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

        with next(get_session()) as session:
            setup_state.mark_complete(session)

        response = self.client.get("/api/setup")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["setup_required"])
        self.assertFalse(response.json()["setup_complete"])

        response = self.client.get("/api/settings/prowlarr")
        self.assertEqual(response.status_code, 200)

        response = self.client.put(
            "/api/settings/prowlarr/base-url",
            json={"base_url": "http://prowlarr:9696"},
        )
        self.assertEqual(response.status_code, 204)

    def test_initialize_login_and_complete_setup_flow(self):
        response = self.client.get("/api/auth/status")
        self.assertEqual(
            response.json(),
            {
                "initialized": False,
                "login_type": None,
                "force_login_type": None,
            },
        )

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

        response = self.client.get("/api/auth/status")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["initialized"])
        self.assertEqual(response.json()["login_type"], "forms")

        for path, body in [
            ("/api/settings/prowlarr/base-url", {"base_url": "http://prowlarr:9696"}),
            ("/api/settings/prowlarr/api-key", {"api_key": "prowlarr-token"}),
            (
                "/api/settings/download-client",
                {
                    "qbit_base_url": "http://qbittorrent:8080",
                    "qbit_user": "admin",
                    "qbit_pass": "secret",
                    "qbit_category": "audiobooks",
                    "qbit_save_path": "/downloads",
                    "qbit_enabled": True,
                    "qbit_complete_action": "copy",
                },
            ),
            (
                "/api/settings/media-management",
                {
                    "library_path": "/library",
                    "folder_pattern": "{author}/{title}",
                    "file_pattern": "{title}",
                    "use_series_folders": False,
                    "use_hardlinks": False,
                    "review_before_import": False,
                },
            ),
        ]:
            method = self.client.patch if path.endswith("download-client") or path.endswith("media-management") else self.client.put
            response = method(path, json=body)
            self.assertEqual(response.status_code, 204, path)

        for path, data in [
            ("/api/settings/audiobookshelf/base-url", {"base_url": "http://abs:13378"}),
            ("/api/settings/audiobookshelf/api-token", {"api_token": "abs-token"}),
            ("/api/settings/audiobookshelf/library", {"library_id": "library-1"}),
            (
                "/api/settings/audiobookshelf/check-downloaded",
                {"check_downloaded": "true"},
            ),
        ]:
            response = self.client.put(path, data=data)
            self.assertEqual(response.status_code, 204, path)

        response = self.client.get("/api/setup/overview")
        self.assertEqual(response.status_code, 200)
        overview = response.json()
        self.assertTrue(overview["auth_initialized"])
        self.assertTrue(overview["prowlarr_ready"])
        self.assertTrue(overview["download_client_ready"])
        self.assertTrue(overview["media_management_ready"])
        self.assertTrue(overview["audiobookshelf_ready"])

        response = self.client.post("/api/setup/complete")
        self.assertEqual(response.status_code, 204)

        response = self.client.get("/api/settings/prowlarr")
        self.assertEqual(response.status_code, 401)

        response = self.client.get("/api/settings/prowlarr", headers=auth_headers)
        self.assertEqual(response.status_code, 200)

    def test_stale_setup_complete_flag_does_not_block_setup_routes(self):
        with next(get_session()) as session:
            setup_state.mark_complete(session)

        response = self.client.get("/api/setup")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["setup_required"])
        self.assertFalse(response.json()["setup_complete"])

        response = self.client.get("/api/settings/prowlarr")
        self.assertEqual(response.status_code, 200)

        response = self.client.put(
            "/api/settings/prowlarr/base-url",
            json={"base_url": "http://prowlarr:9696"},
        )
        self.assertEqual(response.status_code, 204)

    def test_force_setup_override_reports_enabled(self):
        with patch.dict(os.environ, {"ABR_APP__FORCE_SETUP_WIZARD": "true"}):
            response = self.client.get("/api/setup")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["setup_required"])
            self.assertTrue(response.json()["force_setup"])


if __name__ == "__main__":
    unittest.main()
