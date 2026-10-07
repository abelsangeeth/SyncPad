"""Registration, login and token handling."""

from __future__ import annotations

import pytest

from syncpad.api.auth import (
    AuthError,
    color_for,
    create_access_token,
    decode_token,
    hash_password,
    verify_password,
)
from syncpad.config import Settings

# ------------------------------------------------------------------ hashing


def test_password_hashing_round_trip() -> None:
    hashed = hash_password("correct horse battery staple")
    assert hashed != "correct horse battery staple"
    assert verify_password("correct horse battery staple", hashed)
    assert not verify_password("wrong password", hashed)


def test_hashes_are_salted() -> None:
    """Two identical passwords must not produce identical hashes."""
    assert hash_password("same") != hash_password("same")


def test_verifying_against_a_corrupt_hash_is_a_failure_not_a_crash() -> None:
    assert verify_password("anything", "not-a-real-hash") is False


def test_colors_are_deterministic_per_user() -> None:
    assert color_for("user-1") == color_for("user-1")
    assert color_for("user-1").startswith("#")


# ------------------------------------------------------------------- tokens


def test_token_round_trip(settings: Settings) -> None:
    token = create_access_token("user-42", settings)
    assert decode_token(token, settings)["sub"] == "user-42"


def test_token_signed_with_another_secret_is_rejected(settings: Settings) -> None:
    token = create_access_token("user-42", settings)
    other = settings.model_copy(update={"jwt_secret": "a-completely-different-secret"})
    with pytest.raises(AuthError):
        decode_token(token, other)


def test_expired_token_is_rejected(settings: Settings) -> None:
    token = create_access_token("user-42", settings, expires_min=-1)
    with pytest.raises(AuthError):
        decode_token(token, settings)


def test_garbage_token_is_rejected(settings: Settings) -> None:
    with pytest.raises(AuthError):
        decode_token("not.a.token", settings)


# -------------------------------------------------------------------- routes


async def test_register_returns_a_usable_token(client) -> None:
    response = await client.post(
        "/api/auth/register",
        json={"email": "ada@example.com", "name": "Ada", "password": "hunter2-hunter2"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["user"]["email"] == "ada@example.com"
    assert body["user"]["color"].startswith("#")
    assert "password" not in str(body), "no credential material may be echoed back"

    me = await client.get(
        "/api/auth/me", headers={"Authorization": f"Bearer {body['access_token']}"}
    )
    assert me.status_code == 200
    assert me.json()["name"] == "Ada"


async def test_duplicate_registration_is_a_conflict(client) -> None:
    payload = {"email": "dup@example.com", "name": "Dup", "password": "hunter2-hunter2"}
    assert (await client.post("/api/auth/register", json=payload)).status_code == 201
    second = await client.post("/api/auth/register", json=payload)
    assert second.status_code == 409


async def test_email_is_normalised_to_lowercase(client) -> None:
    await client.post(
        "/api/auth/register",
        json={"email": "MiXeD@Example.com", "name": "M", "password": "hunter2-hunter2"},
    )
    login = await client.post(
        "/api/auth/login", json={"email": "mixed@example.com", "password": "hunter2-hunter2"}
    )
    assert login.status_code == 200


@pytest.mark.parametrize(
    "payload",
    [
        {"email": "not-an-email", "name": "X", "password": "hunter2-hunter2"},
        {"email": "a@b.com", "name": "", "password": "hunter2-hunter2"},
        {"email": "a@b.com", "name": "X", "password": "short"},
        {"name": "X", "password": "hunter2-hunter2"},
    ],
)
async def test_registration_validates_input(client, payload: dict) -> None:
    assert (await client.post("/api/auth/register", json=payload)).status_code == 422


async def test_login_succeeds_and_fails_appropriately(client) -> None:
    await client.post(
        "/api/auth/register",
        json={"email": "grace@example.com", "name": "Grace", "password": "hunter2-hunter2"},
    )

    good = await client.post(
        "/api/auth/login", json={"email": "grace@example.com", "password": "hunter2-hunter2"}
    )
    assert good.status_code == 200
    assert good.json()["user"]["name"] == "Grace"

    bad = await client.post(
        "/api/auth/login", json={"email": "grace@example.com", "password": "wrong-password"}
    )
    assert bad.status_code == 401


async def test_login_does_not_reveal_whether_an_account_exists(client) -> None:
    """Both failures must be indistinguishable to a caller enumerating emails."""
    await client.post(
        "/api/auth/register",
        json={"email": "real@example.com", "name": "Real", "password": "hunter2-hunter2"},
    )
    wrong_password = await client.post(
        "/api/auth/login", json={"email": "real@example.com", "password": "nope-nope-nope"}
    )
    no_such_user = await client.post(
        "/api/auth/login", json={"email": "ghost@example.com", "password": "nope-nope-nope"}
    )

    assert wrong_password.status_code == no_such_user.status_code == 401
    assert wrong_password.json() == no_such_user.json()


async def test_me_requires_authentication(client) -> None:
    response = await client.get("/api/auth/me")
    assert response.status_code == 401
    assert "detail" in response.json()


async def test_me_rejects_a_bad_token(client) -> None:
    response = await client.get("/api/auth/me", headers={"Authorization": "Bearer nonsense"})
    assert response.status_code == 401


async def test_me_rejects_a_token_for_a_deleted_user(client, settings: Settings) -> None:
    ghost = create_access_token("00000000-0000-0000-0000-000000000000", settings)
    response = await client.get("/api/auth/me", headers={"Authorization": f"Bearer {ghost}"})
    assert response.status_code == 401
