import logging
import uuid

from datetime import datetime, date, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select, update, and_
from sqlalchemy.orm import Session
from app.common.enums import CommandStatus, EventType, Severity, SystemMode, UserRole
from app.common.timeutils import utcnow
from app.infra.models import BatchRow, BatchMeasurementRow, RejectBinRow, RejectBinItemRow, SettingRow, UserRow, UserSessionRow
from .models import CommandRow, EventRow, SystemStateRow


log = logging.getLogger(__name__)


def _normalize_user_role(role: str | UserRole) -> str:
    value = role.value if isinstance(role, UserRole) else str(role).strip().lower()
    try:
        return UserRole(value).value
    except ValueError as exc:
        raise ValueError(f"unknown user role: {value}") from exc


def create_user(
    db: Session,
    *,
    login: str,
    display_name: str,
    role: str | UserRole,
    password_hash: str,
    is_active: bool = True,
) -> UserRow:
    normalized_login = str(login or "").strip()
    normalized_name = str(display_name or "").strip()
    normalized_password_hash = str(password_hash or "").strip()

    if not normalized_login:
        raise ValueError("user login is empty")
    if not normalized_name:
        raise ValueError("user display_name is empty")
    if not normalized_password_hash:
        raise ValueError("user password_hash is empty")

    row = UserRow(
        login=normalized_login,
        display_name=normalized_name,
        role=_normalize_user_role(role),
        password_hash=normalized_password_hash,
        is_active=bool(is_active),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_user(db: Session, user_id: int) -> UserRow | None:
    return db.get(UserRow, int(user_id))


def get_user_by_login(db: Session, login: str) -> UserRow | None:
    normalized_login = str(login or "").strip()
    if not normalized_login:
        return None
    return (
        db.execute(
            select(UserRow).where(UserRow.login == normalized_login).limit(1)
        )
        .scalar_one_or_none()
    )


def list_users(
    db: Session,
    *,
    role: str | UserRole | None = None,
    active_only: bool = False,
) -> list[UserRow]:
    query = select(UserRow)

    if role is not None:
        query = query.where(UserRow.role == _normalize_user_role(role))
    if active_only:
        query = query.where(UserRow.is_active == True)

    return list(
        db.execute(query.order_by(UserRow.display_name.asc(), UserRow.id.asc()))
        .scalars()
        .all()
    )


def set_user_active(
    db: Session,
    user_id: int,
    *,
    is_active: bool,
) -> UserRow | None:
    row = get_user(db, int(user_id))
    if row is None:
        return None

    row.is_active = bool(is_active)
    db.commit()
    db.refresh(row)
    return row


def create_user_session(
    db: Session,
    *,
    user_id: int,
    token_hash: str,
    expires_at: datetime,
) -> UserSessionRow:
    normalized_hash = str(token_hash or "").strip()
    if not normalized_hash:
        raise ValueError("session token_hash is empty")
    if expires_at is None:
        raise ValueError("session expires_at is required")
    if get_user(db, int(user_id)) is None:
        raise ValueError(f"unknown user id: {user_id}")

    row = UserSessionRow(
        user_id=int(user_id),
        token_hash=normalized_hash,
        expires_at=expires_at,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_user_session_by_token_hash(
    db: Session,
    token_hash: str,
) -> UserSessionRow | None:
    normalized_hash = str(token_hash or "").strip()
    if not normalized_hash:
        return None
    return (
        db.execute(
            select(UserSessionRow)
            .where(UserSessionRow.token_hash == normalized_hash)
            .limit(1)
        )
        .scalar_one_or_none()
    )


def delete_user_session(db: Session, session_id: int) -> bool:
    row = db.get(UserSessionRow, int(session_id))
    if row is None:
        return False

    db.delete(row)
    db.commit()
    return True


def delete_user_sessions_for_user(db: Session, user_id: int) -> int:
    rows = (
        db.execute(
            select(UserSessionRow).where(
                UserSessionRow.user_id == int(user_id)
            )
        )
        .scalars()
        .all()
    )
    for row in rows:
        db.delete(row)

    if rows:
        db.commit()

    return len(rows)


def cleanup_expired_user_sessions(
    db: Session,
    *,
    now: datetime | None = None,
) -> int:
    cutoff = now if now is not None else utcnow()
    rows = (
        db.execute(
            select(UserSessionRow).where(
                UserSessionRow.expires_at <= cutoff
            )
        )
        .scalars()
        .all()
    )
    for row in rows:
        db.delete(row)

    if rows:
        db.commit()

    return len(rows)


OPERATOR_MESSAGE_PREFIX = "opmsg:"
OPERATOR_MESSAGE_KEY_MAX_LENGTH = 64 - len(OPERATOR_MESSAGE_PREFIX)
OPERATOR_MESSAGE_DEFAULT_PRIORITY = {
    Severity.info.value: 100,
    Severity.warn.value: 200,
    Severity.error.value: 300,
}
OPERATOR_MESSAGE_SEVERITY_RANK = {
    Severity.info.value: 0,
    Severity.warn.value: 1,
    Severity.error.value: 2,
}


def _normalize_operator_message_key(message_key: str) -> tuple[str, str]:
    key = str(message_key or "").strip()

    if not key:
        raise ValueError("operator message key is empty")

    if key.startswith(OPERATOR_MESSAGE_PREFIX):
        key = key[len(OPERATOR_MESSAGE_PREFIX):]

    if not key or any(ch.isspace() for ch in key):
        raise ValueError(
            "operator message key must be non-empty and contain no whitespace"
        )

    if len(key) > OPERATOR_MESSAGE_KEY_MAX_LENGTH:
        raise ValueError(
            "operator message key is too long: "
            f"max {OPERATOR_MESSAGE_KEY_MAX_LENGTH} characters"
        )

    return key, f"{OPERATOR_MESSAGE_PREFIX}{key}"


def _normalize_operator_message_severity(
    severity: str | Severity,
) -> str:
    value = severity.value if hasattr(severity, "value") else str(severity)
    value = value.strip().lower()

    if value not in OPERATOR_MESSAGE_DEFAULT_PRIORITY:
        raise ValueError(f"unknown operator message severity: {value}")

    return value


def _operator_message_datetime(value) -> datetime | None:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        result = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return None

    if result.tzinfo is not None:
        result = result.astimezone(timezone.utc).replace(tzinfo=None)

    return result


def _operator_message_from_row(row: SettingRow) -> dict | None:
    value = row.value
    if not isinstance(value, dict):
        return None

    message = str(value.get("message") or "").strip()
    if not message:
        return None

    key = str(row.key)[len(OPERATOR_MESSAGE_PREFIX):]
    if not key:
        return None

    raw_severity = str(
        value.get("severity") or Severity.info.value
    ).strip().lower()
    severity = (
        raw_severity
        if raw_severity in OPERATOR_MESSAGE_DEFAULT_PRIORITY
        else Severity.info.value
    )

    try:
        priority = int(
            value.get(
                "priority",
                OPERATOR_MESSAGE_DEFAULT_PRIORITY[severity],
            )
        )
    except (TypeError, ValueError):
        priority = OPERATOR_MESSAGE_DEFAULT_PRIORITY[severity]

    created_at = (
        _operator_message_datetime(value.get("created_at"))
        or _operator_message_datetime(row.updated_at)
        or utcnow()
    )
    updated_at = (
        _operator_message_datetime(value.get("updated_at"))
        or _operator_message_datetime(row.updated_at)
        or created_at
    )
    expires_at = _operator_message_datetime(value.get("expires_at"))

    return {
        "key": key,
        "message": message,
        "severity": severity,
        "priority": priority,
        "sticky": bool(value.get("sticky", expires_at is None)),
        "created_at": created_at,
        "updated_at": updated_at,
        "expires_at": expires_at,
    }


def _operator_message_sort_key(message: dict) -> tuple:
    updated_at = message.get("updated_at")
    if not isinstance(updated_at, datetime):
        updated_at = datetime.min

    return (
        -int(message.get("priority") or 0),
        -OPERATOR_MESSAGE_SEVERITY_RANK.get(
            str(message.get("severity") or Severity.info.value),
            0,
        ),
        -updated_at.timestamp(),
        str(message.get("key") or ""),
    )


def _load_operator_messages(
    db: Session,
    *,
    now: datetime | None = None,
    delete_expired: bool = False,
) -> tuple[list[dict], int]:
    # Все даты операторских сообщений внутри реестра храним и сравниваем
    # как naive UTC. В разных версиях timeutils.utcnow() мог возвращать
    # как naive, так и timezone-aware datetime, поэтому нормализуем и
    # явно переданный now, и текущее время из utcnow().
    now = (
        _operator_message_datetime(
            now if now is not None else utcnow()
        )
        or datetime.utcnow()
    )
    rows = (
        db.execute(
            select(SettingRow)
            .where(SettingRow.key.like(f"{OPERATOR_MESSAGE_PREFIX}%"))
        )
        .scalars()
        .all()
    )

    messages: list[dict] = []
    expired_count = 0

    for row in rows:
        message = _operator_message_from_row(row)
        if message is None:
            continue

        expires_at = message.get("expires_at")
        if isinstance(expires_at, datetime) and expires_at <= now:
            expired_count += 1
            if delete_expired:
                db.delete(row)
            continue

        messages.append(message)

    messages.sort(key=_operator_message_sort_key)
    return messages, expired_count


def _ensure_state_row_without_commit(
    db: Session,
) -> tuple[SystemStateRow, bool]:
    row = db.get(SystemStateRow, 1)
    if row is not None:
        return row, False

    row = SystemStateRow(
        id=1,
        mode=SystemMode.idle.value,
        updated_at=utcnow(),
    )
    db.add(row)
    return row, True


def _sync_legacy_operator_message(
    db: Session,
    messages: list[dict],
    *,
    clear_when_empty: bool,
) -> bool:
    if not messages and not clear_when_empty:
        return False

    state, changed = _ensure_state_row_without_commit(db)

    if messages:
        primary = messages[0]
        legacy_message = str(primary["message"])
        legacy_severity = str(primary["severity"])
    else:
        legacy_message = None
        legacy_severity = Severity.info.value

    state_changed = False

    if state.message != legacy_message:
        state.message = legacy_message
        state_changed = True
        changed = True

    severity_row = db.get(SettingRow, "message_severity")
    if severity_row is None:
        severity_row = SettingRow(
            key="message_severity",
            value=legacy_severity,
            updated_at=utcnow(),
        )
        db.add(severity_row)
        changed = True
    elif severity_row.value != legacy_severity:
        severity_row.value = legacy_severity
        severity_row.updated_at = utcnow()
        state_changed = True
        changed = True

    if state_changed:
        state.updated_at = utcnow()

    return changed


def publish_operator_message(
    db: Session,
    *,
    message_key: str,
    message: str,
    severity: str | Severity = Severity.info.value,
    priority: int | None = None,
    sticky: bool = True,
    ttl_sec: float | None = None,
    refresh_ttl: bool = False,
) -> dict:
    """
    Создаёт или обновляет одно активное сообщение оператора.

    Каждое сообщение хранится в отдельной SettingRow с ключом
    ``opmsg:<message_key>``. Благодаря этому независимые сессии БД
    не перезаписывают общий JSON-реестр целиком.

    Идентичная повторная публикация не меняет timestamps и не делает commit.
    Для явного продления TTL используйте ``refresh_ttl=True``.
    """
    key, setting_key = _normalize_operator_message_key(message_key)
    text = str(message or "").strip()
    if not text:
        raise ValueError("operator message text is empty")

    severity_value = _normalize_operator_message_severity(severity)
    priority_value = (
        OPERATOR_MESSAGE_DEFAULT_PRIORITY[severity_value]
        if priority is None
        else int(priority)
    )

    ttl_value: float | None = None
    if ttl_sec is not None:
        ttl_value = float(ttl_sec)
        if ttl_value <= 0:
            raise ValueError("operator message ttl_sec must be greater than zero")
        sticky = False

    # Используем тот же формат времени, что и
    # _operator_message_datetime(): naive UTC.
    now = (
        _operator_message_datetime(utcnow())
        or datetime.utcnow()
    )
    row = db.get(SettingRow, setting_key)
    existing = _operator_message_from_row(row) if row is not None else None

    existing_is_active = bool(
        existing
        and (
            existing.get("expires_at") is None
            or existing["expires_at"] > now
        )
    )

    same_content = bool(
        existing_is_active
        and existing["message"] == text
        and existing["severity"] == severity_value
        and int(existing["priority"]) == priority_value
        and bool(existing["sticky"]) == bool(sticky)
        and (
            (ttl_value is None and existing["expires_at"] is None)
            or (
                ttl_value is not None
                and existing["expires_at"] is not None
            )
        )
    )

    row_changed = False

    if not same_content or (ttl_value is not None and refresh_ttl):
        created_at = (
            existing["created_at"]
            if existing is not None
            else now
        )
        expires_at = (
            now + timedelta(seconds=ttl_value)
            if ttl_value is not None
            else None
        )
        payload = {
            "message": text,
            "severity": severity_value,
            "priority": priority_value,
            "sticky": bool(sticky),
            "created_at": created_at.isoformat(),
            "updated_at": now.isoformat(),
            "expires_at": (
                expires_at.isoformat()
                if expires_at is not None
                else None
            ),
        }

        if row is None:
            row = SettingRow(
                key=setting_key,
                value=payload,
                updated_at=now,
            )
            db.add(row)
        else:
            row.value = payload
            row.updated_at = now

        row_changed = True

    # SessionLocal в проекте может работать с autoflush=False.
    # Новая/изменённая SettingRow должна попасть в БД до запроса
    # _load_operator_messages(), иначе запрос вернёт старый список,
    # хотя последующий commit уже сохранит сообщение.
    if row_changed:
        db.flush()

    messages, expired_count = _load_operator_messages(
        db,
        now=now,
        delete_expired=True,
    )
    legacy_changed = _sync_legacy_operator_message(
        db,
        messages,
        clear_when_empty=bool(expired_count),
    )

    if row_changed or expired_count or legacy_changed:
        db.commit()

    for current in messages:
        if current["key"] == key:
            return current

    # Сюда можно попасть только при некорректной внешней модификации строки.
    raise RuntimeError(f"operator message was not stored: {key}")


def clear_operator_message(
    db: Session,
    message_key: str,
) -> bool:
    """Удаляет только указанное активное сообщение оператора."""
    _key, setting_key = _normalize_operator_message_key(message_key)
    row = db.get(SettingRow, setting_key)

    if row is None:
        return False

    db.delete(row)
    db.flush()

    messages, expired_count = _load_operator_messages(
        db,
        now=utcnow(),
        delete_expired=True,
    )
    _sync_legacy_operator_message(
        db,
        messages,
        clear_when_empty=True,
    )
    db.commit()
    return True


def cleanup_expired_operator_messages(
    db: Session,
    *,
    now: datetime | None = None,
) -> int:
    """Удаляет просроченные сообщения и обновляет legacy-поля."""
    messages, expired_count = _load_operator_messages(
        db,
        now=now or utcnow(),
        delete_expired=True,
    )

    if expired_count:
        _sync_legacy_operator_message(
            db,
            messages,
            clear_when_empty=True,
        )
        db.commit()

    return expired_count


def list_operator_messages(
    db: Session,
    *,
    cleanup_expired: bool = True,
) -> list[dict]:
    """
    Возвращает активные сообщения по убыванию приоритета.

    При включённой очистке просроченные строки удаляются в той же транзакции,
    после чего главное сообщение зеркалируется в legacy-поля.
    """
    messages, expired_count = _load_operator_messages(
        db,
        now=utcnow(),
        delete_expired=bool(cleanup_expired),
    )

    legacy_changed = _sync_legacy_operator_message(
        db,
        messages,
        clear_when_empty=bool(expired_count),
    )

    if expired_count or legacy_changed:
        db.commit()

    return messages


def _json_safe(x):
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, dict):
        return {str(k): _json_safe(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_json_safe(v) for v in x]
    return x

def ensure_state_row(db: Session):
    row = db.get(SystemStateRow, 1)
    if not row:
        row = SystemStateRow(id=1, mode=SystemMode.idle.value, updated_at=utcnow())
        db.add(row)
        db.commit()
    return row


def add_event(db: Session, *, severity: str, source: str, type_: str, payload: dict):
    ev = EventRow(severity=severity, source=source, type=type_, payload=payload)
    db.add(ev)
    db.commit()
    return ev


def add_user_audit_event(
    db: Session,
    *,
    type_: str,
    user_id: int,
    display_name: str,
    role: str,
    target: dict | None = None,
    result: str = "success",
    details: dict | None = None,
    severity: str = Severity.info.value,
    source: str = "WEB",
    legacy_payload: dict | None = None,
) -> EventRow | None:
    """Best-effort user/admin audit event with one stable payload shape.

    The user action itself must never depend on this secondary audit write.
    All current callers invoke this helper only after the primary operation
    has already been committed. If the audit insert fails, the failure is
    logged and swallowed instead of changing the result of the user action.
    """
    payload = {
        "user_id": int(user_id),
        "display_name": str(display_name),
        "role": str(role),
        "action": str(type_),
        "target": _json_safe(target or {}),
        "result": str(result),
        "details": _json_safe(details or {}),
    }

    # Keep legacy top-level keys for existing consumers while new code can
    # rely on the normalized fields above.
    if legacy_payload:
        payload.update(_json_safe(legacy_payload))

    try:
        return add_event(
            db,
            severity=str(severity),
            source=str(source),
            type_=str(type_),
            payload=payload,
        )
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        log.exception("user audit write failed: %s", type_)
        return None


def set_state(db: Session, **patch):
    row = ensure_state_row(db)
    for k, v in patch.items():
        setattr(row, k, v)
    row.updated_at = utcnow()
    db.commit()
    return row


def new_operation_id() -> str:
    return uuid.uuid4().hex


def begin_active_operation(
    db: Session,
    *,
    batch_id: int,
    expected_count: int | None = None,
    phase: str = "starting",
    operation_id: str | None = None,
) -> str:
    """
    Старт нового валидного контекста автоцикла.
    Любые async-команды ИМ/РТК должны нести этот operation_id.
    """
    op_id = str(operation_id or new_operation_id())

    set_state(
        db,
        active_batch_id=int(batch_id),
        active_batch_phase=str(phase),
        active_batch_expected_count=(
            None if expected_count is None else int(expected_count)
        ),
        active_operation_id=op_id,
        active_operation_batch_id=int(batch_id),
        active_operation_phase=str(phase),
        active_operation_started_at=utcnow(),

        # сбрасываем глобальные gate-результаты старой операции
        pending_mm_result=None,
        mm_result_inflight=0,
        pending_calib_result=None,
        calib_result_inflight=0,
        calibration_inflight=0,
        calibration_reason=None,
        calib_wait_im_program=0,
        calib_im_program_target=None,
    )

    return op_id


def clear_active_operation(
    db: Session,
    *,
    clear_active_batch: bool = False,
):
    patch = {
        "active_operation_id": None,
        "active_operation_batch_id": None,
        "active_operation_phase": None,
        "active_operation_started_at": None,

        "pending_mm_result": None,
        "mm_result_inflight": 0,
        "pending_calib_result": None,
        "calib_result_inflight": 0,
        "calibration_inflight": 0,
        "calibration_reason": None,
        "calib_wait_im_program": 0,
        "calib_im_program_target": None,
    }

    if clear_active_batch:
        patch.update(
            {
                "active_batch_id": None,
                "active_batch_phase": None,
                "active_batch_expected_count": None,
            }
        )

    return set_state(db, **patch)


def active_operation_matches(
    db: Session,
    *,
    batch_id: int | str | None,
    operation_id: str | None,
) -> bool:
    if batch_id is None:
        return False

    op_id = str(operation_id or "").strip()
    if not op_id:
        return False

    st = ensure_state_row(db)

    active_op_id = str(getattr(st, "active_operation_id", None) or "").strip()
    if not active_op_id:
        return False

    try:
        bid = int(batch_id)
    except Exception:
        return False

    active_bid = getattr(st, "active_operation_batch_id", None)
    if active_bid is None:
        active_bid = getattr(st, "active_batch_id", None)

    try:
        active_bid = int(active_bid)
    except Exception:
        return False

    return active_op_id == op_id and active_bid == bid


def claim_next_command(db: Session) -> CommandRow | None:
    # SQLite-safe: выбираем pending, потом атомарно переводим в running
    cmd = db.execute(
        select(CommandRow).where(CommandRow.status == CommandStatus.pending.value).order_by(CommandRow.id.asc()).limit(1)
    ).scalar_one_or_none()
    if not cmd:
        return None

    updated = db.execute(
        update(CommandRow)
        .where(CommandRow.id == cmd.id, CommandRow.status == CommandStatus.pending.value)
        .values(status=CommandStatus.running.value)
    )
    db.commit()
    if updated.rowcount == 1:
        db.refresh(cmd)
        return cmd
    return None


def finish_command(db: Session, cmd_id: int, ok: bool, error: str | None = None):
    st = CommandStatus.done.value if ok else CommandStatus.failed.value
    db.execute(update(CommandRow).where(CommandRow.id == cmd_id).values(status=st, error=error))
    db.commit()


def get_active_reject_bin(db: Session) -> RejectBinRow | None:
    return db.execute(select(RejectBinRow).where(RejectBinRow.is_active == True).order_by(RejectBinRow.id.desc()).limit(1)).scalar_one_or_none()


def ensure_active_reject_bin(db: Session, capacity: int) -> RejectBinRow:
    rb = get_active_reject_bin(db)
    if rb:
        return rb
    rb = RejectBinRow(capacity=int(capacity), is_active=True)
    db.add(rb)
    db.commit()
    db.refresh(rb)
    return rb


def close_reject_bin(db: Session, rb_id: int, *, count_at_close: int, reason: str):
    db.execute(
        update(RejectBinRow)
        .where(RejectBinRow.id == rb_id)
        .values(is_active=False, closed_at=utcnow(), count_at_close=int(count_at_close), close_reason=reason)
    )
    db.commit()


def open_reject_bin(db: Session, capacity: int) -> RejectBinRow:
    rb = RejectBinRow(capacity=int(capacity), is_active=True)
    db.add(rb)
    db.commit()
    db.refresh(rb)
    return rb


def list_reject_bin_items(db: Session, rb_id: int) -> list[RejectBinItemRow]:
    return db.execute(select(RejectBinItemRow).where(RejectBinItemRow.reject_bin_id == rb_id).order_by(RejectBinItemRow.id.asc())).scalars().all()


def list_reject_items_by_batch(db: Session, batch_id: int) -> list[RejectBinItemRow]:
    q = (
        select(RejectBinItemRow)
        .where(RejectBinItemRow.batch_id == int(batch_id))
        .order_by(RejectBinItemRow.id.asc())
    )
    return list(db.execute(q).scalars().all())


def add_reject_bin_item(db: Session, rb_id: int, *, batch_id: int | None, measured_params: dict, reason: str | None):
    it = RejectBinItemRow(
        reject_bin_id=rb_id,
        batch_id=batch_id,
        measured_params=measured_params or {},
        reason=reason,
    )
    db.add(it)
    db.commit()
    db.refresh(it)
    return it


def add_reject_item(
    db: Session,
    *,
    batch_id: int | None,
    values: dict | None = None,
    tols: dict | None = None,
    measured_params: dict | None = None,
    reason: str | None = None,
):
    """
    Кладёт брак в активную тару.
    measured_params можно передать готовым, либо собрать из values/tols.
    """
    st = ensure_state_row(db)
    capacity = int(st.reject_bin_capacity or 0)
    rb = ensure_active_reject_bin(db, capacity)

    if measured_params is None:
        measured_params = {
            "values": values or {},
            "tols": tols or {},
        }

    it = add_reject_bin_item(
        db,
        rb.id,
        batch_id=batch_id,
        measured_params=measured_params,
        reason=reason,
    )

    st2 = ensure_state_row(db)
    new_count = int(st2.reject_bin_count or 0) + 1
    set_state(db, reject_bin_count=new_count, reject_bin_capacity=capacity)

    return it


def add_batch_measurement(
    db: Session,
    *,
    batch_id: int | None,
    part_ok: bool,
    not_ok: int,
    values: dict | None,
    tols: dict | None,
    items: list | None,
    counted: bool | None = None,
):
    """Пишем запись каждого измерения ИМ (для трассировки/аналитики)."""
    row = BatchMeasurementRow(
        batch_id=(int(batch_id) if batch_id is not None else None),
        part_ok=1 if bool(part_ok) else 0,
        not_ok=int(not_ok or 0),
        counted=(None if counted is None else (1 if bool(counted) else 0)),
        values=values or {},
        tols=tols or {},
        items=items or [],
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def create_batch(
    db: Session,
    *,
    data: dict,
    location: dict | None = None,
    status: str = "loaded",
    source: str = "WEB",
) -> BatchRow:
    b = BatchRow(
        status=status,
        data=data or {},
        location=location or {},
        loaded_at=utcnow() if status == "loaded" else None,
    )
    db.add(b)
    db.commit()
    db.refresh(b)

    # событие создания партии
    payload = {
        "batch_id": b.id,
        "status": b.status,
        "product_code": (b.data or {}).get("product_code"),
        "product_count": (b.data or {}).get("product_count"),
        "cell_no": (b.location or {}).get("cell_no"),
        "in_tare_ids": (b.data or {}).get("in_tare_ids") or (b.location or {}).get("in_tare_ids") or [],
        "out_tare_ids": (b.data or {}).get("out_tare_ids") or (b.location or {}).get("out_tare_ids") or [],
    }
    add_event(
        db,
        severity=Severity.info.value,
        source=source,
        type_=EventType.BATCH_CREATED.value,
        payload=payload,
    )

    return b


def get_batch(db: Session, batch_id: int) -> BatchRow | None:
    #return db.get(BatchRow, int(batch_id))

    # важно: без populate_existing можно получить stale-объект из identity map
    # и перетереть batch.data (например out_tares) при последующих commit'ах
    b = db.get(BatchRow, int(batch_id))
    if b:
        try:
            db.refresh(b)
        except Exception:
            pass
    return b    


def set_batch_status(
    db: Session,
    batch_id: int,
    *,
    status: str,
    reject_reason: str | None = None,
    loaded_at=None,
    finished_at=None,
    extracted_at=None,
):
    values = {"status": status, "updated_at": utcnow()}
    if reject_reason is not None:
        values["reject_reason"] = reject_reason
    if loaded_at is not None:
        values["loaded_at"] = loaded_at
    if finished_at is not None:
        values["finished_at"] = finished_at
    if extracted_at is not None:
        values["extracted_at"] = extracted_at

    db.execute(update(BatchRow).where(BatchRow.id == int(batch_id)).values(**values))
    db.commit()


def claim_next_loaded_batch(db: Session) -> BatchRow | None:
    return db.execute(
        select(BatchRow)
        .where(BatchRow.status == "loaded")
        .order_by(BatchRow.id.asc())
        .limit(1)
    ).scalar_one_or_none()



def list_batches_by_status(db: Session, status: str, *, limit: int = 50) -> list[BatchRow]:
    return (
        db.execute(
            select(BatchRow)
            .where(BatchRow.status == str(status))
            .order_by(BatchRow.id.asc())
            .limit(int(limit))
        )
        .scalars()
        .all()
    )


def get_first_batch_by_status(db: Session, status: str) -> BatchRow | None:
    return (
        db.execute(
            select(BatchRow)
            .where(BatchRow.status == str(status))
            .order_by(BatchRow.id.asc())
            .limit(1)
        )
        .scalar_one_or_none()
    )


def inc_batch_measurement(db: Session, batch_id: int, *, part_ok: bool) -> BatchRow | None:
    """
    Инкремент счётчиков партии по факту измерения ИМ:
      - batches.measured_good / batches.measured_bad
      - и дублируем в data: measured_qty/ok_qty/nok_qty + measured_good/measured_bad
    """
    b = get_batch(db, int(batch_id))
    if not b:
        return None

    mg = int(getattr(b, "measured_good", 0) or 0)
    mb = int(getattr(b, "measured_bad", 0) or 0)
    if part_ok:
        mg += 1
    else:
        mb += 1
    b.measured_good = mg
    b.measured_bad = mb

    data = dict(b.data or {})
    data["measured_good"] = mg
    data["measured_bad"] = mb
    data["measured_qty"] = mg + mb
    data["ok_qty"] = mg
    data["nok_qty"] = mb

    if not data.get("started_ts"):
        data["started_ts"] = utcnow().isoformat()
    data["updated_ts"] = utcnow().isoformat()

    b.data = data
    b.updated_at = utcnow()
    db.commit()
    db.refresh(b)
    return b


def list_batches(db: Session, *, status: str | None = None, limit: int = 50) -> list[BatchRow]:
    q = select(BatchRow)
    if status:
        q = q.where(BatchRow.status == status)
    q = q.order_by(BatchRow.id.desc()).limit(int(limit))
    return db.execute(q).scalars().all()


def get_inflight_auto_batch(db: Session) -> BatchRow | None:
    return db.execute(
        select(BatchRow)
        .where(BatchRow.status == "auto_processing")
        .order_by(BatchRow.id.asc())
        .limit(1)
    ).scalar_one_or_none()


def get_setting(db: Session, key: str, default=None):
    row = db.get(SettingRow, key)
    return default if row is None else row.value


def set_setting(db: Session, key: str, value):
    row = db.get(SettingRow, key)
    if row is None:
        row = SettingRow(key=key, value=value, updated_at=utcnow())
        db.add(row)
    else:
        row.value = value
        row.updated_at = utcnow()
    db.commit()
    return row


def get_all_settings(db: Session) -> dict:
    rows = db.execute(select(SettingRow)).scalars().all()
    return {r.key: r.value for r in rows}
