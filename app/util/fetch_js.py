"""
To fetch javascript files for local development
"""

from pathlib import Path

import requests

files = {
    "htmx-preload.js": "https://unpkg.com/htmx-ext-preload@2.1.0/preload.js",
    "htmx.js": "https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js",
    "alpine.js": "https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js",
    "toastify.js": "https://cdn.jsdelivr.net/npm/toastify-js",
    "toastify.css": "https://cdn.jsdelivr.net/npm/toastify-js/src/toastify.min.css",
}

TIMEOUT_SECONDS = 10


def _download_file(path: Path, url: str) -> None:
    try:
        response = requests.get(url, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch {path.name} from {url}: {e}") from e
    path.write_text(response.text)


def _ensure_debug_files(root: Path) -> None:
    if all((root / file_name).exists() for file_name in files.keys()):
        return

    for file_name, url in files.items():
        _download_file(root / file_name, url)


def _ensure_prod_files(root: Path) -> None:
    for file_name in files.keys():
        if not (root / file_name).exists():
            raise FileNotFoundError(
                f"{file_name} must be present in static directory for production. This is most likely an error with the docker image."
            )


def fetch_scripts(debug: bool) -> None:
    root = Path("static")
    root.mkdir(parents=True, exist_ok=True)

    if debug:
        _ensure_debug_files(root)
    else:
        _ensure_prod_files(root)


if __name__ == "__main__":
    fetch_scripts(True)
