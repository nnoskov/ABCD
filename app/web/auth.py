import base64
import hashlib
import hmac
import os
import secrets

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.common.enums import EventType, UserRole
from app.common.timeutils import utcnow
from app.infra.db import SessionLocal
from app.infra.models import UserRow
from app.infra import repo


router = APIRouter(prefix="/auth", tags=["auth"])
admin_router = APIRouter(prefix="/admin", tags=["admin"])

SESSION_COOKIE_NAME = "postamats_session"
SESSION_TOKEN_BYTES = 32
SESSION_TTL_HOURS_DEFAULT = 12
PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 600_000
PASSWORD_SALT_BYTES = 16


class OperatorOut(BaseModel):
    id: int
    display_name: str


class OperatorLoginRequest(BaseModel):
    user_id: int
    password: str = Field(min_length=1, max_length=1024)


class AdminLoginRequest(BaseModel):
    login: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)


class CurrentUserOut(BaseModel):
    id: int
    login: str
    display_name: str
    role: UserRole


class LoginResult(BaseModel):
    ok: bool = True
    user: CurrentUserOut


class AdminOperatorOut(BaseModel):
    id: int
    display_name: str
    full_name: str


class AdminOperatorCreateRequest(BaseModel):
    last_name: str = Field(min_length=1, max_length=64)
    first_name: str = Field(min_length=1, max_length=64)
    middle_name: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=1024)
    password_confirm: str = Field(min_length=1, max_length=1024)


def get_auth_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)

    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _session_ttl_hours() -> int:
    raw = os.getenv(
        "POSTAMATS_SESSION_TTL_HOURS",
        str(SESSION_TTL_HOURS_DEFAULT),
    )
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return SESSION_TTL_HOURS_DEFAULT

    # Не допускаем случайно бессрочные или почти мгновенные сессии.
    return min(max(value, 1), 24 * 30)


def _as_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _now_utc() -> datetime:
    return _as_naive_utc(utcnow())


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value.encode("ascii"))


def hash_password(password: str) -> str:
    raw_password = str(password)
    if not raw_password:
        raise ValueError("password is empty")

    salt = secrets.token_bytes(PASSWORD_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return "$".join(
        (
            PASSWORD_HASH_ALGORITHM,
            str(PASSWORD_HASH_ITERATIONS),
            _b64encode(salt),
            _b64encode(digest),
        )
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations_raw, salt_raw, expected_raw = str(
            password_hash or ""
        ).split("$", 3)
        if algorithm != PASSWORD_HASH_ALGORITHM:
            return False

        iterations = int(iterations_raw)
        if iterations <= 0:
            return False

        salt = _b64decode(salt_raw)
        expected = _b64decode(expected_raw)
        actual = hashlib.pbkdf2_hmac(
            "sha256",
            str(password).encode("utf-8"),
            salt,
            iterations,
            dklen=len(expected),
        )
        return hmac.compare_digest(actual, expected)
    except (TypeError, ValueError, UnicodeError):
        return False


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def _short_display_name(value: str) -> str:
    """Сокращённое имя для пользовательских интерфейсов.

    Новые операторы хранятся как "Фамилия Имя Отчество". Старые
    тестовые/служебные имена оставляем как есть, чтобы не ломать
    совместимость с уже созданными учетными записями.
    """
    normalized = _normalize_display_name(value)
    parts = normalized.split()
    if len(parts) < 3:
        return normalized

    last_name, first_name, middle_name = parts[0], parts[1], parts[2]
    if not first_name or not middle_name:
        return normalized

    return f"{last_name} {first_name[0]}.{middle_name[0]}."


def _current_user_out(user: UserRow) -> CurrentUserOut:
    return CurrentUserOut(
        id=int(user.id),
        login=str(user.login),
        display_name=_short_display_name(str(user.display_name)),
        role=UserRole(str(user.role)),
    )


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=_session_ttl_hours() * 60 * 60,
        httponly=True,
        secure=_env_bool("POSTAMATS_SESSION_COOKIE_SECURE", False),
        samesite="lax",
        path="/",
    )


def _delete_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        secure=_env_bool("POSTAMATS_SESSION_COOKIE_SECURE", False),
        httponly=True,
        samesite="lax",
    )


def _create_session(db: Session, user: UserRow) -> str:
    expires_at = _now_utc() + timedelta(hours=_session_ttl_hours())

    # Коллизия SHA-256 для случайных 256-битных токенов практически
    # невозможна, но небольшой retry оставляет функцию корректной даже
    # при искусственно созданной коллизии в тестовой БД.
    for _ in range(3):
        token = secrets.token_urlsafe(SESSION_TOKEN_BYTES)
        try:
            repo.create_user_session(
                db,
                user_id=int(user.id),
                token_hash=_token_hash(token),
                expires_at=expires_at,
            )
            return token
        except IntegrityError:
            db.rollback()

    raise RuntimeError("unable to create unique user session")


def get_current_user_optional(
    request: Request,
    db: Session = Depends(get_auth_db),
) -> UserRow | None:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None

    session = repo.get_user_session_by_token_hash(db, _token_hash(token))
    if session is None:
        return None

    now = _now_utc()
    expires_at = _as_naive_utc(session.expires_at)
    if expires_at <= now:
        repo.delete_user_session(db, int(session.id))
        return None

    user = repo.get_user(db, int(session.user_id))
    if user is None or not bool(user.is_active):
        repo.delete_user_session(db, int(session.id))
        return None

    try:
        UserRole(str(user.role))
    except ValueError:
        repo.delete_user_session(db, int(session.id))
        return None

    return user


def require_current_user(
    user: UserRow | None = Depends(get_current_user_optional),
) -> UserRow:
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация.",
        )
    return user


def require_admin(
    user: UserRow = Depends(require_current_user),
) -> UserRow:
    if str(user.role) != UserRole.admin.value:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Действие доступно только администратору.",
        )
    return user


def _normalize_display_name(value: str) -> str:
    # Для UI сохраняем обычное представление имени, но убираем случайные
    # повторные/краевые пробелы. Это также делает проверку дубликатов
    # предсказуемой.
    return " ".join(str(value or "").split())


def _display_name_key(value: str) -> str:
    return _normalize_display_name(value).casefold()


def _operator_name_exists(db: Session, display_name: str) -> bool:
    target = _display_name_key(display_name)
    return any(
        _display_name_key(user.display_name) == target
        for user in repo.list_users(
            db,
            role=UserRole.operator,
            active_only=False,
        )
    )


def _create_operator_user(
    db: Session,
    *,
    display_name: str,
    password: str,
) -> UserRow:
    password_hash = hash_password(password)

    # Оператор выбирается по id из /auth/operators, поэтому отдельный
    # пользовательский login ему не нужен. Генерируем внутренний login,
    # не показываем его в UI и не принимаем из браузера.
    for _ in range(3):
        login = f"operator-{secrets.token_hex(12)}"
        try:
            return repo.create_user(
                db,
                login=login,
                display_name=display_name,
                role=UserRole.operator,
                password_hash=password_hash,
                is_active=True,
            )
        except IntegrityError:
            db.rollback()

    raise RuntimeError("unable to create unique operator login")


def initialize_auth() -> None:
    """Очистить истёкшие сессии и при необходимости создать bootstrap-admin."""
    db = SessionLocal()
    try:
        repo.cleanup_expired_user_sessions(db, now=_now_utc())

        active_admins = repo.list_users(
            db,
            role=UserRole.admin,
            active_only=True,
        )
        if active_admins:
            return

        login = str(os.getenv("POSTAMATS_ADMIN_LOGIN") or "").strip()
        password = str(os.getenv("POSTAMATS_ADMIN_PASSWORD") or "")
        display_name = str(os.getenv("POSTAMATS_ADMIN_NAME") or "").strip()

        missing = []
        if not login:
            missing.append("POSTAMATS_ADMIN_LOGIN")
        if not password:
            missing.append("POSTAMATS_ADMIN_PASSWORD")
        if not display_name:
            missing.append("POSTAMATS_ADMIN_NAME")

        if missing:
            print(
                "AUTH: active admin is missing; bootstrap-admin was not "
                "created because environment variables are missing: "
                + ", ".join(missing)
            )
            return

        existing = repo.get_user_by_login(db, login)
        if existing is not None:
            print(
                "AUTH: active admin is missing; bootstrap-admin was not "
                f"created because login {login!r} already exists"
            )
            return

        try:
            repo.create_user(
                db,
                login=login,
                display_name=display_name,
                role=UserRole.admin,
                password_hash=hash_password(password),
                is_active=True,
            )
        except IntegrityError:
            # Защита от гонки при запуске нескольких web-workers.
            db.rollback()
            active_admins = repo.list_users(
                db,
                role=UserRole.admin,
                active_only=True,
            )
            if active_admins:
                return
            raise

        print(f"AUTH: bootstrap-admin created for login {login!r}")
    finally:
        db.close()


@router.get("/operators", response_model=list[OperatorOut])
def list_active_operators(
    db: Session = Depends(get_auth_db),
):
    users = repo.list_users(
        db,
        role=UserRole.operator,
        active_only=True,
    )
    return [
        OperatorOut(
            id=int(user.id),
            display_name=_short_display_name(str(user.display_name)),
        )
        for user in users
    ]


@router.post("/login/operator", response_model=LoginResult)
def login_operator(
    body: OperatorLoginRequest,
    response: Response,
    db: Session = Depends(get_auth_db),
):
    user = repo.get_user(db, int(body.user_id))
    if (
        user is None
        or not bool(user.is_active)
        or str(user.role) != UserRole.operator.value
        or not verify_password(body.password, user.password_hash)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный пользователь или пароль.",
        )

    repo.cleanup_expired_user_sessions(db, now=_now_utc())
    token = _create_session(db, user)
    _set_session_cookie(response, token)

    repo.add_user_audit_event(
        db,
        type_=EventType.USER_LOGIN.value,
        user_id=int(user.id),
        display_name=str(user.display_name),
        role=str(user.role),
        target={"type": "session"},
        details={"login_kind": "operator"},
    )

    return LoginResult(user=_current_user_out(user))


@router.post("/login/admin", response_model=LoginResult)
def login_admin(
    body: AdminLoginRequest,
    response: Response,
    db: Session = Depends(get_auth_db),
):
    login = str(body.login or "").strip()
    user = repo.get_user_by_login(db, login)
    if (
        user is None
        or not bool(user.is_active)
        or str(user.role) != UserRole.admin.value
        or not verify_password(body.password, user.password_hash)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный логин или пароль.",
        )

    repo.cleanup_expired_user_sessions(db, now=_now_utc())
    token = _create_session(db, user)
    _set_session_cookie(response, token)

    repo.add_user_audit_event(
        db,
        type_=EventType.USER_LOGIN.value,
        user_id=int(user.id),
        display_name=str(user.display_name),
        role=str(user.role),
        target={"type": "session"},
        details={"login_kind": "admin"},
    )

    return LoginResult(user=_current_user_out(user))


@router.post("/logout")
def logout(
    request: Request,
    response: Response,
    db: Session = Depends(get_auth_db),
):
    token = request.cookies.get(SESSION_COOKIE_NAME)
    audit_user = None
    if token:
        session = repo.get_user_session_by_token_hash(db, _token_hash(token))
        if session is not None:
            audit_user = repo.get_user(db, int(session.user_id))
            repo.delete_user_session(db, int(session.id))

    if audit_user is not None:
        repo.add_user_audit_event(
            db,
            type_=EventType.USER_LOGOUT.value,
            user_id=int(audit_user.id),
            display_name=str(audit_user.display_name),
            role=str(audit_user.role),
            target={"type": "session"},
        )

    _delete_session_cookie(response)
    return {"ok": True}


@admin_router.get("/operators", response_model=list[AdminOperatorOut])
def admin_list_operators(
    db: Session = Depends(get_auth_db),
    _current_admin: UserRow = Depends(require_admin),
):
    users = repo.list_users(
        db,
        role=UserRole.operator,
        active_only=True,
    )
    return [
        AdminOperatorOut(
            id=int(user.id),
            display_name=_short_display_name(str(user.display_name)),
            full_name=str(user.display_name),
        )
        for user in users
    ]


@admin_router.post(
    "/operators",
    response_model=AdminOperatorOut,
    status_code=status.HTTP_201_CREATED,
)
def admin_create_operator(
    body: AdminOperatorCreateRequest,
    db: Session = Depends(get_auth_db),
    current_admin: UserRow = Depends(require_admin),
):
    last_name = _normalize_display_name(body.last_name)
    first_name = _normalize_display_name(body.first_name)
    middle_name = _normalize_display_name(body.middle_name)

    missing = []
    if not last_name:
        missing.append("фамилию")
    if not first_name:
        missing.append("имя")
    if not middle_name:
        missing.append("отчество")
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Укажите " + ", ".join(missing) + " оператора.",
        )

    display_name = _normalize_display_name(
        f"{last_name} {first_name} {middle_name}"
    )
    if len(display_name) > 128:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="ФИО оператора слишком длинное.",
        )

    if body.password != body.password_confirm:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Пароли не совпадают.",
        )

    if _operator_name_exists(db, display_name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Оператор с таким именем уже существует.",
        )

    try:
        user = _create_operator_user(
            db,
            display_name=display_name,
            password=body.password,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    repo.add_user_audit_event(
        db,
        type_=EventType.OPERATOR_CREATED.value,
        user_id=int(current_admin.id),
        display_name=str(current_admin.display_name),
        role=str(current_admin.role),
        target={
            "type": "operator",
            "user_id": int(user.id),
            "display_name": str(user.display_name),
        },
        details={
            "operator_role": str(user.role),
        },
        legacy_payload={
            "admin_user_id": int(current_admin.id),
            "admin_display_name": str(current_admin.display_name),
            "admin_role": str(current_admin.role),
            "operator_user_id": int(user.id),
            "operator_display_name": str(user.display_name),
        },
    )

    return AdminOperatorOut(
        id=int(user.id),
        display_name=_short_display_name(str(user.display_name)),
        full_name=str(user.display_name),
    )


@admin_router.delete("/operators/{user_id}")
def admin_disable_operator(
    user_id: int,
    db: Session = Depends(get_auth_db),
    current_admin: UserRow = Depends(require_admin),
):
    user = repo.get_user(db, int(user_id))
    if user is None or str(user.role) != UserRole.operator.value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Оператор не найден.",
        )

    if not bool(user.is_active):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Оператор уже удален.",
        )

    repo.set_user_active(
        db,
        int(user.id),
        is_active=False,
    )
    invalidated_sessions = repo.delete_user_sessions_for_user(
        db,
        int(user.id),
    )

    repo.add_user_audit_event(
        db,
        type_=EventType.OPERATOR_DISABLED.value,
        user_id=int(current_admin.id),
        display_name=str(current_admin.display_name),
        role=str(current_admin.role),
        target={
            "type": "operator",
            "user_id": int(user.id),
            "display_name": str(user.display_name),
        },
        details={
            "invalidated_sessions": int(invalidated_sessions),
        },
        legacy_payload={
            "admin_user_id": int(current_admin.id),
            "admin_display_name": str(current_admin.display_name),
            "admin_role": str(current_admin.role),
            "operator_user_id": int(user.id),
            "operator_display_name": str(user.display_name),
            "invalidated_sessions": int(invalidated_sessions),
        },
    )

    return {
        "ok": True,
        "id": int(user.id),
        "display_name": str(user.display_name),
        "is_active": False,
        "invalidated_sessions": int(invalidated_sessions),
    }


@router.get("/me", response_model=CurrentUserOut)
def auth_me(
    user: UserRow = Depends(require_current_user),
):
    return _current_user_out(user)
