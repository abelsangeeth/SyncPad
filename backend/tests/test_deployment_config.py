"""Two ways a deployment can look healthy while being wrong.

Both of these were found by deploying the container rather than by running the
suite, which is why they get tests now.

1. The frontend was located by walking up from ``__file__``, which is correct in
   a source checkout and wrong in an installed package. Every container returned
   404 on ``/`` while every API route worked. The compose stack hid it, because
   nginx proxies ``/`` to the gateway instead of serving files.

2. ``SYNCPAD_DATABASE_URL`` has a SQLite default. Unset in production on a
   container host, the server starts, reports itself ready, saves documents to
   the container filesystem, and loses all of them on the next restart.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from syncpad.config import DEFAULT_DATABASE_URL, Settings
from syncpad.main import find_frontend

POSTGRES = "postgresql://u:p@host:5432/db"


# --- locating the frontend ---------------------------------------------


def _make_frontend(root) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.html").write_text("<!doctype html>", encoding="utf-8")


def test_explicit_directory_wins(tmp_path) -> None:
    _make_frontend(tmp_path / "chosen")
    assert find_frontend(tmp_path / "chosen") == tmp_path / "chosen"


def test_source_checkout_layout_is_found(tmp_path) -> None:
    """<root>/backend/syncpad/main.py next to <root>/frontend."""
    module = tmp_path / "backend" / "syncpad" / "main.py"
    module.parent.mkdir(parents=True)
    module.write_text("", encoding="utf-8")
    _make_frontend(tmp_path / "frontend")

    assert find_frontend(module=module, cwd=tmp_path / "nowhere") == tmp_path / "frontend"


def test_container_layout_is_found(tmp_path) -> None:
    """The case that was broken: /app/syncpad/main.py with /app/frontend.

    ``parents[2]`` of that module is the filesystem root, so the source-checkout
    candidate cannot match and the working directory has to be consulted.
    """
    app = tmp_path / "app"
    module = app / "syncpad" / "main.py"
    module.parent.mkdir(parents=True)
    module.write_text("", encoding="utf-8")
    _make_frontend(app / "frontend")

    assert find_frontend(module=module, cwd=app) == app / "frontend"


def test_installed_beside_the_package_is_found(tmp_path) -> None:
    pkg = tmp_path / "site-packages" / "syncpad"
    module = pkg / "main.py"
    pkg.mkdir(parents=True)
    module.write_text("", encoding="utf-8")
    _make_frontend(pkg.parent / "frontend")

    assert find_frontend(module=module, cwd=tmp_path / "nowhere") == pkg.parent / "frontend"


def test_absent_frontend_returns_none_rather_than_a_bad_mount(tmp_path) -> None:
    module = tmp_path / "a" / "b" / "main.py"
    module.parent.mkdir(parents=True)
    module.write_text("", encoding="utf-8")

    assert find_frontend(module=module, cwd=tmp_path) is None


def test_a_directory_without_index_html_does_not_count(tmp_path) -> None:
    """Mounting an empty directory would serve 404s while looking configured."""
    (tmp_path / "chosen").mkdir()
    module = tmp_path / "a" / "b" / "main.py"
    module.parent.mkdir(parents=True)
    module.write_text("", encoding="utf-8")

    assert find_frontend(tmp_path / "chosen", module=module, cwd=tmp_path) is None


def test_the_real_repository_layout_resolves() -> None:
    """Guards the developer path the other tests simulate."""
    found = find_frontend()
    assert found is not None
    assert (found / "index.html").is_file()


# --- refusing to run prod on the dev default ---------------------------


def test_prod_rejects_the_unset_database_url() -> None:
    with pytest.raises(ValidationError) as caught:
        Settings(env="prod")
    assert "SYNCPAD_DATABASE_URL" in str(caught.value)


def test_prod_accepts_an_explicit_database_url() -> None:
    assert Settings(env="prod", database_url=POSTGRES).database_url == POSTGRES


@pytest.mark.parametrize("env", ["dev", "test"])
def test_the_default_is_still_fine_outside_prod(env: str) -> None:
    """Zero-config local runs must keep working; that is the point of it."""
    assert Settings(env=env).database_url == DEFAULT_DATABASE_URL


def test_an_explicit_sqlite_path_is_allowed_in_prod() -> None:
    """A single-host deployment with a real volume is legitimate.

    The check compares values, so it cannot distinguish "deliberately chose the
    default string" from "chose nothing". Any other path is accepted, which is
    the documented way to say you meant it.
    """
    url = "sqlite+aiosqlite:////var/lib/syncpad/syncpad.db"
    assert Settings(env="prod", database_url=url).database_url == url
