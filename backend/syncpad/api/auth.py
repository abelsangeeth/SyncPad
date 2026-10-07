"""Registration, login, and the dependencies that identify a caller.

Passwords are hashed with PBKDF2-SHA256 rather than bcrypt. Not because it is
better — bcrypt is the usual first choice — but because passlib's bcrypt backend
is broken against current bcrypt releases, and a hashing scheme that raises on
import is worse than one that is merely conventional. PBKDF2 is in the standard
library, has no version coupling, and at the iteration count below is a
defensible choice.

Tokens are short JWTs carrying nothing but a subject and an expiry. The
WebSocket gateway validates them with the same :func:`decode_token`, so there is
exactly one place where a credential is turned into an identity.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr, Field

from syncpad.config import Settings
from syncpad.storage import DocumentRepo, User

__all__ = [
    "AuthError",
    "color_for",
    "create_access_token",
    "current_user",
    "decode_token",
    "hash_password",
    "optional_user",
    "router",
    "verify_password",
]

log = logging.getLogger("syncpad.auth")

_pwd = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto", pbkdf2_sha256__rounds=210_000)

#: auto_error=False so a missing header becomes "anonymous" rather than a 403,
#: which optional_user needs in order to exist at all.
_bearer = HTTPBearer(auto_error=False)

router = APIRouter(prefix="/api/auth", tags=["auth"])

#: Presence colours. Assigned deterministically from the user id so a person
#: looks the same in every document and on every device.
PALETTE = (
    "#ef4444",
    "#f97316",
    "#f59e0b",
    "#84cc16",
    "#22c55e",
    "#14b8a6",
    "#06b6d4",
    "#3b82f6",
    "#6366f1",
    "#8b5cf6",
    "#d946ef",
    "#ec4899",
)


class AuthError(Exception):
    """A credential was missing, malformed, or expired."""


def color_for(user_id: str) -> str:
    digest = hashlib.sha256(user_id.encode("utf-8")).digest()
    return PALETTE[digest[0] % len(PALETTE)]


def hash_password(password: str) -> str:
    return _pwd.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    try:
        return _pwd.verify(password, hashed)
    except ValueError:
        # A corrupt or foreign hash is a failed login, not a 500.
        return False


def create_access_token(sub: str, settings: Settings, expires_min: int | None = None) -> str:
    minutes = expires_min if expires_min is not None else settings.access_token_minutes
    now = datetime.now(UTC)
    claims = {
        "sub": sub,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=minutes)).timestamp()),
    }
    return jwt.encode(claims, settings.jwt_secret, algorithm=settings.jwt_alg)


def decode_token(token: str, settings: Settings) -> dict[str, Any]:
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])
    except JWTError as exc:
        raise AuthError(str(exc)) from exc


# --------------------------------------------------------------- schemas


class RegisterIn(BaseModel):
    email: EmailStr
    name: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=8, max_length=256)


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut


class UserOut(BaseModel):
    id: str
    email: str
    name: str
    color: str

    @classmethod
    def of(cls, user: User) -> UserOut:
        return cls(id=user.id, email=user.email, name=user.name, color=user.color)


TokenOut.model_rebuild()


# ------------------------------------------------------------ dependencies


async def _repo(request: Request) -> DocumentRepo:
    """A repo bound to a per-request session, committed by the route."""
    return DocumentRepo(request.state.session)


async def optional_user(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)] = None,
) -> User | None:
    """The authenticated user, or None. Never raises on a bad token."""
    if credentials is None or not credentials.credentials:
        return None
    settings: Settings = request.app.state.settings
    try:
        claims = decode_token(credentials.credentials, settings)
    except AuthError:
        return None
    repo = DocumentRepo(request.state.session)
    return await repo.user_by_id(str(claims.get("sub", "")))


async def current_user(
    user: Annotated[User | None, Depends(optional_user)],
) -> User:
    """The authenticated user, or a clean 401."""
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


CurrentUser = Annotated[User, Depends(current_user)]
OptionalUser = Annotated[User | None, Depends(optional_user)]


# ----------------------------------------------------------------- routes


@router.post("/register", response_model=TokenOut, status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterIn, request: Request) -> TokenOut:
    repo = DocumentRepo(request.state.session)
    if await repo.user_by_email(payload.email) is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "that email is already registered")

    user = await repo.create_user(
        email=payload.email,
        name=payload.name,
        password_hash=hash_password(payload.password),
        color="#6366f1",
    )
    # The colour depends on the generated id, so it is set once that exists.
    user.color = color_for(user.id)
    await request.state.session.commit()

    settings: Settings = request.app.state.settings
    return TokenOut(access_token=create_access_token(user.id, settings), user=UserOut.of(user))


@router.post("/login", response_model=TokenOut)
async def login(payload: LoginIn, request: Request) -> TokenOut:
    repo = DocumentRepo(request.state.session)
    user = await repo.user_by_email(payload.email)

    # Same response and roughly the same work whether the account exists or the
    # password is wrong, so the endpoint does not enumerate registered emails.
    if user is None or not verify_password(payload.password, user.password_hash):
        if user is None:
            hash_password(payload.password)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid email or password")

    settings: Settings = request.app.state.settings
    return TokenOut(access_token=create_access_token(user.id, settings), user=UserOut.of(user))


@router.get("/me", response_model=UserOut)
async def me(user: CurrentUser) -> UserOut:
    return UserOut.of(user)
