import asyncio
import json
import time
import csv
import io
import os
import math
import ipaddress
import termios

from datetime import datetime
from typing import Any, Optional, List, Dict
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from pydantic import BaseModel, Field, StrictInt

from app.infra.db import SessionLocal
from app.infra.models import (
    CommandRow,
    EventRow,
    SystemStateRow,
    BatchRow,
    BatchMeasurementRow,
    RejectBinItemRow,
    UserRow,
)
from app.infra import repo
from app.web.auth import get_current_user_optional, require_admin, require_current_user

from app.common.schemas import CommandCreate, CommandOut, EventOut, SystemState
from app.common.enums import CommandStatus, CommandType, EventType, Severity, UserRole
from app.common.runtime_paths import get_temperature_snapshot_path
from app.common.product_rules import (
    ProductRuleError,
    resolve_product_rule,
    uses_special_unloading_cell,
)

router = APIRouter()

SPECIAL_UNLOADING_CELL = 16

DEFAULT_TEMP_NEAR_CRITICAL = 2.0
DEFAULT_TEMP_LOADING_MIN = 15.0
DEFAULT_TEMP_LOADING_MAX = 25.0
DEFAULT_TEMP_IM_MIN = 15.0
DEFAULT_TEMP_IM_MAX = 25.0
DEFAULT_CONSECUTIVE_REJECTS_THRESHOLD = 3
DEFAULT_BATCH_LAYOUT = 2
DEFAULT_USE_ALTERNATE_WAVE = True
STOP_AFTER_BATCH_SETTING = "stop_after_batch_id"

QR_SERIAL_DEVICE = os.getenv("QR_SERIAL_DEVICE", "/dev/ttyACM0")
QR_SERIAL_TIMEOUT_SEC = 15.0
QR_SERIAL_IDLE_SEC = 0.25
QR_SERIAL_POLL_SEC = 0.02
QR_SERIAL_MAX_BYTES = 20000

QR_FIELD_MAP = {
    "rpn": "passport_number",
    "rpd": "passport_date",
    "al": "blank_alloy",
    "bl": "blank_name",
    "tsi": "prod_tsi",
    "tsb": "prod_tsb",
    "name": "product_name",
    "pn": "product_code",
    "rev": "draw_rev",
    "cert": "cert_number",
    "lot": "rod_batch_number",
    "w": "items_mass",
    "qty": "product_count",
    "fe": "prod_fe",
    "opt1": "prod_opt1",
    "opt2": "prod_opt2",
    "opt3": "prod_opt3",
    "opt4": "prod_opt4",
    "comnt": "prod_comment",
}

_qr_serial_lock = asyncio.Lock()

# Команды, которые являются частью штатного производственного интерфейса.
# operator и admin могут ставить их через общий /api/commands.
# Остальные известные CommandType через этот endpoint доступны только admin.
ABORT_ACTIVE_BATCH_FOR_EXTRACTION = "ABORT_ACTIVE_BATCH_FOR_EXTRACTION"

OPERATOR_COMMAND_TYPES = frozenset(
    {
        CommandType.START_AUTO.value,
        CommandType.STOP_AUTO.value,
        CommandType.PAUSE_SYSTEM.value,
        CommandType.RESUME_SYSTEM.value,
        CommandType.REPLACE_REJECTBIN.value,
        CommandType.RTK_CYCLE_ON.value,
        CommandType.RTK_SET_STEP_MODE.value,
        CommandType.RTK_NEXT_STEP.value,
        CommandType.SET_ACTIVE_BATCH.value,
        CommandType.MARK_ACTIVE_BATCH_EXTRACTED.value,
        CommandType.PRINT_DOCS.value,
        CommandType.POSTAMAT_OPEN_LOADING_CELL.value,
        CommandType.POSTAMAT_OPEN_UNLOADING_CELL.value,
        CommandType.CHECK_BATCH_EXTRACTION_DOORS.value,
        ABORT_ACTIVE_BATCH_FOR_EXTRACTION,
    }
)

# Неизвестные строки больше не должны попадать в CommandRow.
# Все значения enum считаются известными; специальная STOP-команда пока
# исторически реализована в Supervisor строкой и поэтому добавлена отдельно.
KNOWN_WEB_COMMAND_TYPES = frozenset(
    {command.value for command in CommandType} | {ABORT_ACTIVE_BATCH_FOR_EXTRACTION}
)

TEMPERATURE_SNAPSHOT_FILE = get_temperature_snapshot_path()

try:
    TEMPERATURE_SNAPSHOT_STALE_SEC = max(
        1.0,
        float(
            os.getenv(
                "TEMPERATURE_SNAPSHOT_STALE_SEC",
                "5.0",
            )
        ),
    )
except (TypeError, ValueError):
    TEMPERATURE_SNAPSHOT_STALE_SEC = 5.0


def _empty_temperature_snapshot() -> dict:
    return {
        "updated_at_ts": None,
        "stale": True,
        "overall_status": "missing",
        "loading": {
            "value": None,
            "status": "missing",
        },
        "im": {
            "value": None,
            "status": "missing",
        },
    }


def _read_temperature_snapshot() -> dict:
    if not TEMPERATURE_SNAPSHOT_FILE:
        return _empty_temperature_snapshot()

    try:
        with open(
            TEMPERATURE_SNAPSHOT_FILE,
            "r",
            encoding="utf-8",
        ) as fh:
            data = json.load(fh)

        updated_at_ts = float(data.get("updated_at_ts"))
        if not math.isfinite(updated_at_ts):
            raise ValueError("invalid updated_at_ts")

        stale = time.time() - updated_at_ts > TEMPERATURE_SNAPSHOT_STALE_SEC

        if stale:
            return _empty_temperature_snapshot()

        def _sensor(name: str) -> dict:
            raw = data.get(name) or {}
            value = raw.get("value")
            if value is not None:
                value = float(value)
                if not math.isfinite(value):
                    value = None

            status = str(raw.get("status") or "unknown")
            if status not in {
                "ok",
                "near_critical",
                "out_of_range",
                "missing",
            }:
                status = "unknown"

            return {
                "value": value,
                "status": status,
            }

        return {
            "updated_at_ts": updated_at_ts,
            "stale": False,
            "overall_status": str(data.get("overall_status") or "unknown"),
            "loading": _sensor("loading"),
            "im": _sensor("im"),
        }

    except Exception:
        return _empty_temperature_snapshot()


# Партия физически продолжает занимать ячейку выгрузки,
# пока оператор не подтвердил её извлечение.
OCCUPIED_BATCH_STATUSES = {
    "blocked",
    "loaded",
    "auto_processing",
    "done",
    "rejected",
}


def _normalize_product_code(value: str | None) -> str:
    return str(value or "").strip()


def _required_batch_text(value, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise HTTPException(
            status_code=422,
            detail=f"Поле «{label}» обязательно для заполнения.",
        )
    return normalized


def _normalize_passport_date(value) -> str:
    raw = _required_batch_text(value, "Дата маршрутного паспорта")

    try:
        parsed = datetime.strptime(raw, "%d/%m/%Y")
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=422,
            detail=(
                "Дата маршрутного паспорта должна быть введена в формате "
                "дд/мм/гггг и содержать корректную календарную дату."
            ),
        )

    return parsed.strftime("%d/%m/%Y")


def _parse_qr_serial_payload(raw: str) -> dict[str, str]:
    try:
        pairs = parse_qsl(
            raw,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=64,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"QR-код имеет некорректный формат: {exc}",
        ) from exc

    result: dict[str, str] = {}
    seen_keys: set[str] = set()

    for qr_key, value in pairs:
        system_key = QR_FIELD_MAP.get(qr_key)
        if system_key is None:
            continue

        if qr_key in seen_keys:
            raise HTTPException(
                status_code=422,
                detail=f"QR-код содержит повторяющийся параметр: {qr_key}",
            )

        seen_keys.add(qr_key)
        result[system_key] = value

    if not result:
        raise HTTPException(
            status_code=422,
            detail="QR-код не содержит поддерживаемых параметров.",
        )

    return result


async def _read_qr_serial_line(request: Request) -> str:
    fd: int | None = None
    original_attrs = None

    try:
        try:
            fd = os.open(
                QR_SERIAL_DEVICE,
                os.O_RDONLY | os.O_NOCTTY | os.O_NONBLOCK,
            )
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"QR-сканер не найден: {QR_SERIAL_DEVICE}",
            ) from exc
        except PermissionError as exc:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Нет доступа к QR-сканеру {QR_SERIAL_DEVICE}. "
                    "Проверьте права пользователя web-сервиса на COM-устройство."
                ),
            ) from exc
        except OSError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Не удалось открыть QR-сканер {QR_SERIAL_DEVICE}: {exc}",
            ) from exc

        try:
            original_attrs = termios.tcgetattr(fd)
            scan_attrs = termios.tcgetattr(fd)
            scan_attrs[3] &= ~(termios.ICANON | termios.ECHO)
            scan_attrs[6][termios.VMIN] = 0
            scan_attrs[6][termios.VTIME] = 0
            termios.tcsetattr(fd, termios.TCSANOW, scan_attrs)
            termios.tcflush(fd, termios.TCIFLUSH)
        except termios.error:
            # Для CDC ACM это обычно обычный TTY. Если конкретный драйвер
            # не поддерживает termios, всё равно пробуем неблокирующее чтение.
            original_attrs = None

        loop = asyncio.get_running_loop()
        deadline = loop.time() + QR_SERIAL_TIMEOUT_SEC
        last_data_at: float | None = None
        buffer = bytearray()

        while True:
            if await request.is_disconnected():
                raise HTTPException(
                    status_code=499,
                    detail="Ожидание QR-кода отменено.",
                )

            try:
                chunk = os.read(fd, 4096)
            except BlockingIOError:
                chunk = b""
            except OSError as exc:
                raise HTTPException(
                    status_code=503,
                    detail=f"Ошибка чтения QR-сканера {QR_SERIAL_DEVICE}: {exc}",
                ) from exc

            now = loop.time()

            if chunk:
                buffer.extend(chunk)
                last_data_at = now

                if len(buffer) > QR_SERIAL_MAX_BYTES:
                    raise HTTPException(
                        status_code=422,
                        detail=(
                            "QR-код слишком большой: допустимо не более "
                            f"{QR_SERIAL_MAX_BYTES} байт."
                        ),
                    )

                if b"\n" in buffer or b"\r" in buffer:
                    break

            elif (
                buffer
                and last_data_at is not None
                and (now - last_data_at) >= QR_SERIAL_IDLE_SEC
            ):
                # Резерв для сканеров без завершающего CR/LF.
                break

            if now >= deadline:
                if not buffer:
                    raise HTTPException(
                        status_code=408,
                        detail=(
                            "Время ожидания QR-кода истекло. "
                            "Нажмите «Сканировать QR» и повторите."
                        ),
                    )
                break

            await asyncio.sleep(QR_SERIAL_POLL_SEC)

        payload = bytes(buffer).strip(b"\x00\r\n\t ")
        if not payload:
            raise HTTPException(
                status_code=422,
                detail="QR-сканер вернул пустую строку.",
            )

        try:
            return payload.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=422,
                detail="QR-код получен не в UTF-8 кодировке.",
            ) from exc

    finally:
        if fd is not None:
            if original_attrs is not None:
                try:
                    termios.tcsetattr(fd, termios.TCSANOW, original_attrs)
                except (termios.error, OSError):
                    pass

            try:
                os.close(fd)
            except OSError:
                pass


def _batch_tare_ids(batch: BatchRow, *, side: str) -> list[int]:
    data = batch.data or {}
    location = batch.location or {}

    product_code = _normalize_product_code(data.get("product_code"))

    # Бизнес-правило имеет приоритет над сохранёнными данными:
    # специальная деталь всегда выгружается только через ячейку 16.
    if side == "unloading" and uses_special_unloading_cell(product_code):
        return [int(SPECIAL_UNLOADING_CELL)]

    if side == "loading":
        raw = data.get("in_tare_ids")
        if not raw:
            raw = location.get("in_tare_ids")
    elif side == "unloading":
        raw = data.get("out_tare_ids")
        if not raw:
            raw = location.get("out_tare_ids")
    else:
        raise ValueError(f"unknown side: {side}")

    # location.cell_no — исторически номер загрузочной ячейки.
    if not raw:
        cell_no = location.get("cell_no") or data.get("cell_no")
        raw = [] if cell_no is None else [cell_no]

    if not isinstance(raw, list):
        raw = [raw]

    result: list[int] = []

    for value in raw:
        try:
            cell_no = int(value)
        except (TypeError, ValueError):
            continue

        if cell_no > 0 and cell_no not in result:
            result.append(cell_no)

    return result


def _unloading_cell_is_occupied(db: Session, cell_no: int) -> bool:
    batches = (
        db.execute(select(BatchRow).where(BatchRow.status.in_(OCCUPIED_BATCH_STATUSES)))
        .scalars()
        .all()
    )

    return any(
        int(cell_no) in _batch_tare_ids(batch, side="unloading") for batch in batches
    )


def _derive_product_spec(
    product_code: str | None, product_spec: int | None
) -> int | None:
    if product_spec is not None:
        try:
            return int(product_spec)
        except Exception:
            return None
    if not product_code:
        return None
    pc = str(product_code).strip()
    try:
        from app.daemon.utils import parse_product_name_and_spec

        _, spec = parse_product_name_and_spec(pc)
        return int(spec) if spec is not None else None
    except Exception:
        pass
    try:
        return int(pc.split(".")[-1])
    except Exception:
        return None


class SettingsPatch(BaseModel):
    reject_bin_capacity: Optional[int] = None
    tick_period: Optional[float] = None
    layout: Optional[StrictInt] = Field(default=None, ge=0, le=3)
    use_alternate_wave: Optional[bool] = None

    # одноразовый останов после текущей партии
    stop_after_current_batch: Optional[bool] = None

    # допустимая доля брака [0..1]
    settings_ppod: Optional[float] = None

    # количество браков подряд до запуска проверки
    consecutive_rejects_threshold: Optional[StrictInt] = Field(
        default=None,
        gt=0,
    )

    rjb_near_full: Optional[int] = None
    temp_near_critical: Optional[float] = None

    temperature_sensor_loading_min: Optional[float] = None
    temperature_sensor_loading_max: Optional[float] = None
    temperature_sensor_im_min: Optional[float] = None
    temperature_sensor_im_max: Optional[float] = None

    # UDP-передача данных извлечённой партии
    udp_server_ip: Optional[str] = None
    udp_server_port: Optional[StrictInt] = Field(
        default=None,
        ge=1,
        le=65535,
    )


class BatchCreate(BaseModel):
    """Схема создания партии (паспортная информация хранится в data(JSON))."""

    passport_number: str | None = None
    passport_date: str | None = None

    product_code: str | None = None
    product_name: str | None = None
    product_spec: int | None = (
        None  # если не передали - попробуем получить из product_code
    )

    blank_alloy: str | None = None
    blank_name: str | None = None
    cert_number: str | None = None
    rod_batch_number: str | None = None
    # blank_number: str | None = None
    items_mass: float | None = None
    prod_tsi: str | None = None
    prod_tsb: str | None = None
    draw_rev: str | None = None
    prod_fe: str | None = None
    prod_opt1: str | None = None
    prod_opt2: str | None = None
    prod_opt3: str | None = None
    prod_opt4: str | None = None
    prod_comment: str | None = None

    product_count: int = Field(..., ge=1)
    cell_no: int = Field(..., ge=1)


class BatchOut(BaseModel):
    id: int
    status: str
    data: dict
    location: dict
    loaded_at: str | None = None
    finished_at: str | None = None
    extracted_at: str | None = None


class CommandCreateLoose(BaseModel):
    type: str
    payload: Optional[dict[str, Any]] = None


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _authorize_web_command(
    db: Session,
    *,
    command_type: str,
    current_user: UserRow,
) -> str:
    cmd_type = str(command_type or "").strip()

    if not cmd_type or cmd_type not in KNOWN_WEB_COMMAND_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Неизвестный тип команды: {cmd_type or '-'}",
        )

    role = UserRole(str(current_user.role))

    if role == UserRole.operator and cmd_type not in OPERATOR_COMMAND_TYPES:
        raise HTTPException(
            status_code=403,
            detail="Команда доступна только администратору.",
        )

    return cmd_type


def _enqueue_cmd(
    db: Session,
    *,
    type_: str,
    payload: dict,
    created_by: str,
) -> CommandRow:
    cmd = CommandRow(
        type=type_,
        status=CommandStatus.pending.value,
        payload=payload,
        created_by=str(created_by),
    )
    db.add(cmd)
    db.commit()
    db.refresh(cmd)

    db.add(
        EventRow(
            severity=Severity.info.value,
            source="WEB",
            type=EventType.COMMAND_ACCEPTED.value,
            payload={
                "command_id": cmd.id,
                "type": cmd.type,
                "created_by": cmd.created_by,
            },
        )
    )
    db.commit()
    return cmd


def _tare_ids_from_batch(b: BatchRow) -> tuple[list[int], list[int]]:
    return (
        _batch_tare_ids(b, side="loading"),
        _batch_tare_ids(b, side="unloading"),
    )


def _batch_out_from_row(batch: BatchRow) -> BatchOut:
    data = dict(batch.data or {})
    location = dict(batch.location or {})

    in_ids = _batch_tare_ids(batch, side="loading")
    out_ids = _batch_tare_ids(batch, side="unloading")

    # В API всегда отдаём нормализованные значения.
    data["in_tare_ids"] = [int(x) for x in in_ids]
    data["out_tare_ids"] = [int(x) for x in out_ids]

    location["in_tare_ids"] = [int(x) for x in in_ids]
    location["out_tare_ids"] = [int(x) for x in out_ids]

    # cell_no остаётся номером загрузочной ячейки.
    if location.get("cell_no") is not None:
        try:
            location["cell_no"] = int(location["cell_no"])
        except (TypeError, ValueError):
            pass

    return BatchOut(
        id=int(batch.id),
        status=batch.status,
        data=data,
        location=location,
        loaded_at=(None if not batch.loaded_at else batch.loaded_at.isoformat()),
        finished_at=(None if not batch.finished_at else batch.finished_at.isoformat()),
        extracted_at=(
            None if not batch.extracted_at else batch.extracted_at.isoformat()
        ),
    )


@router.get("/settings")
def get_settings(
    db: Session = Depends(get_db),
    _current_user: UserRow = Depends(require_current_user),
):
    st = repo.ensure_state_row(db)
    s = repo.get_all_settings(db)
    temperature_snapshot = _read_temperature_snapshot()

    # Показываем номер именно физически установленной активной тары.
    # rejectbin_replacement_tare_no использовать здесь нельзя:
    # во время замены там уже может находиться номер новой тары,
    # хотя старая тара ещё физически не извлечена.
    reject_bin_tare_no = None

    try:
        active_reject_bin = repo.ensure_active_reject_bin(
            db,
            int(st.reject_bin_capacity or 0),
        )

        raw_tare_no = getattr(
            active_reject_bin,
            "tare_no",
            None,
        )

        if raw_tare_no is not None:
            tare_no = int(raw_tare_no)

            if 1 <= tare_no <= 10:
                reject_bin_tare_no = tare_no

    except Exception:
        # Отсутствие номера не должно ломать /settings.
        reject_bin_tare_no = None

    raw_stop_after_batch_id = s.get(
        STOP_AFTER_BATCH_SETTING,
        0,
    )

    try:
        stop_after_batch_id = int(raw_stop_after_batch_id or 0)
    except (TypeError, ValueError):
        stop_after_batch_id = 0

    if stop_after_batch_id <= 0:
        stop_after_batch_id = None

    try:
        layout = int(
            s.get(
                "layout",
                DEFAULT_BATCH_LAYOUT,
            )
        )
    except (TypeError, ValueError):
        layout = DEFAULT_BATCH_LAYOUT

    if layout not in (0, 1, 2, 3):
        layout = DEFAULT_BATCH_LAYOUT

    return {
        "reject_bin_capacity": int(st.reject_bin_capacity or 0),
        "reject_bin_tare_no": reject_bin_tare_no,
        "tick_period": s.get("tick_period"),
        "layout": layout,
        "use_alternate_wave": bool(
            s.get(
                "use_alternate_wave",
                DEFAULT_USE_ALTERNATE_WAVE,
            )
        ),
        "stop_after_current_batch": bool(stop_after_batch_id),
        "stop_after_batch_id": stop_after_batch_id,
        "settings_ppod": s.get("settings_ppod", 1.0),
        "consecutive_rejects_threshold": s.get(
            "consecutive_rejects_threshold",
            DEFAULT_CONSECUTIVE_REJECTS_THRESHOLD,
        ),
        "rjb_near_full": s.get("rjb_near_full", 1),
        "temp_near_critical": s.get(
            "temp_near_critical",
            DEFAULT_TEMP_NEAR_CRITICAL,
        ),
        "temperature_sensor_loading_min": s.get(
            "temperature_sensor_loading_min",
            DEFAULT_TEMP_LOADING_MIN,
        ),
        "temperature_sensor_loading_max": s.get(
            "temperature_sensor_loading_max",
            DEFAULT_TEMP_LOADING_MAX,
        ),
        "temperature_sensor_im_min": s.get(
            "temperature_sensor_im_min",
            DEFAULT_TEMP_IM_MIN,
        ),
        "temperature_sensor_im_max": s.get(
            "temperature_sensor_im_max",
            DEFAULT_TEMP_IM_MAX,
        ),
        "temperature_sensor_loading_value": (temperature_snapshot["loading"]["value"]),
        "temperature_sensor_loading_status": (
            temperature_snapshot["loading"]["status"]
        ),
        "temperature_sensor_im_value": (temperature_snapshot["im"]["value"]),
        "temperature_sensor_im_status": (temperature_snapshot["im"]["status"]),
        "temperature_status": temperature_snapshot["overall_status"],
        "temperature_snapshot_updated_at_ts": (temperature_snapshot["updated_at_ts"]),
        "temperature_snapshot_stale": bool(temperature_snapshot["stale"]),
        "udp_server_ip": str(s.get("udp_server_ip", "") or ""),
        "udp_server_port": s.get(
            "udp_server_port",
            None,
        ),
        "air_pressure_ok": bool(s.get("air_pressure_ok", True)),
        "rtk_connected_r1": bool(s.get("rtk_connected_r1", False)),
        "rtk_connected_r2": bool(s.get("rtk_connected_r2", False)),
        "im_connected": bool(s.get("im_connected", False)),
        "im_loaded_program": str(s.get("im_loaded_program", "") or ""),
        "emergency_active": bool(s.get("emergency_active", False)),
        "emergency_sound_muted": bool(s.get("emergency_sound_muted", False)),
    }


@router.put("/settings")
def patch_settings(
    body: SettingsPatch,
    db: Session = Depends(get_db),
    current_user: UserRow = Depends(require_current_user),
):
    by = str(current_user.display_name)
    patch = {}

    # Этап 5: оператору доступно только оперативное управление
    # остановом после текущей партии. Любые остальные поля настроек
    # должны быть отклонены до валидации и до записи в БД.
    if str(current_user.role) != UserRole.admin.value:
        requested_fields = set(body.model_fields_set)
        operator_request_allowed = (
            requested_fields == {"stop_after_current_batch"}
            and body.stop_after_current_batch is not None
        )
        if not operator_request_allowed:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Изменение системных настроек доступно " "только администратору."
                ),
            )

    current_settings = repo.get_all_settings(db)

    def _validated_udp_ip(value) -> str:
        raw = str(value or "").strip()
        if not raw:
            raise HTTPException(
                status_code=422,
                detail="IP-адрес UDP-сервера обязателен",
            )

        try:
            address = ipaddress.ip_address(raw)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="IP-адрес UDP-сервера указан некорректно",
            )

        if address.is_unspecified:
            raise HTTPException(
                status_code=422,
                detail=("IP-адрес UDP-сервера не может быть " "неопределённым адресом"),
            )

        if address.is_multicast:
            raise HTTPException(
                status_code=422,
                detail=("IP-адрес UDP-сервера не может быть " "групповым адресом"),
            )

        return str(address)

    def _validated_udp_port(value) -> int:
        if isinstance(value, bool):
            raise HTTPException(
                status_code=422,
                detail="Порт UDP-сервера должен быть целым числом",
            )

        try:
            port = int(value)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=422,
                detail="Порт UDP-сервера должен быть целым числом",
            )

        if not 1 <= port <= 65535:
            raise HTTPException(
                status_code=422,
                detail=("Порт UDP-сервера должен находиться " "в диапазоне 1..65535"),
            )

        return port

    def _finite_temperature_value(
        name: str,
        value,
    ) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=422,
                detail=f"{name} должно быть числом",
            )

        if not math.isfinite(result):
            raise HTTPException(
                status_code=422,
                detail=f"{name} должно быть конечным числом",
            )

        return result

    temperature_patch_requested = any(
        value is not None
        for value in (
            body.temp_near_critical,
            body.temperature_sensor_loading_min,
            body.temperature_sensor_loading_max,
            body.temperature_sensor_im_min,
            body.temperature_sensor_im_max,
        )
    )

    if temperature_patch_requested:
        temp_near = _finite_temperature_value(
            "temp_near_critical",
            (
                body.temp_near_critical
                if body.temp_near_critical is not None
                else current_settings.get(
                    "temp_near_critical",
                    DEFAULT_TEMP_NEAR_CRITICAL,
                )
            ),
        )
        loading_min = _finite_temperature_value(
            "temperature_sensor_loading_min",
            (
                body.temperature_sensor_loading_min
                if body.temperature_sensor_loading_min is not None
                else current_settings.get(
                    "temperature_sensor_loading_min",
                    DEFAULT_TEMP_LOADING_MIN,
                )
            ),
        )
        loading_max = _finite_temperature_value(
            "temperature_sensor_loading_max",
            (
                body.temperature_sensor_loading_max
                if body.temperature_sensor_loading_max is not None
                else current_settings.get(
                    "temperature_sensor_loading_max",
                    DEFAULT_TEMP_LOADING_MAX,
                )
            ),
        )
        im_min = _finite_temperature_value(
            "temperature_sensor_im_min",
            (
                body.temperature_sensor_im_min
                if body.temperature_sensor_im_min is not None
                else current_settings.get(
                    "temperature_sensor_im_min",
                    DEFAULT_TEMP_IM_MIN,
                )
            ),
        )
        im_max = _finite_temperature_value(
            "temperature_sensor_im_max",
            (
                body.temperature_sensor_im_max
                if body.temperature_sensor_im_max is not None
                else current_settings.get(
                    "temperature_sensor_im_max",
                    DEFAULT_TEMP_IM_MAX,
                )
            ),
        )

        if temp_near < 0:
            raise HTTPException(
                status_code=422,
                detail="temp_near_critical не может быть отрицательным",
            )

        if loading_min >= loading_max:
            raise HTTPException(
                status_code=422,
                detail=(
                    "temperature_sensor_loading_min должно быть "
                    "меньше temperature_sensor_loading_max"
                ),
            )

        if im_min >= im_max:
            raise HTTPException(
                status_code=422,
                detail=(
                    "temperature_sensor_im_min должно быть "
                    "меньше temperature_sensor_im_max"
                ),
            )

    if body.reject_bin_capacity is not None:
        cap = max(0, int(body.reject_bin_capacity))
        repo.set_state(db, reject_bin_capacity=cap)
        patch["reject_bin_capacity"] = cap

    if body.tick_period is not None:
        tp = float(body.tick_period)
        if tp > 0:
            repo.set_setting(db, "tick_period", tp)
            patch["tick_period"] = tp

    if body.layout is not None:
        layout = int(body.layout)
        repo.set_setting(db, "layout", layout)
        patch["layout"] = layout

    if body.use_alternate_wave is not None:
        value = bool(body.use_alternate_wave)
        repo.set_setting(db, "use_alternate_wave", value)
        patch["use_alternate_wave"] = value

    udp_patch_requested = (
        body.udp_server_ip is not None or body.udp_server_port is not None
    )

    if udp_patch_requested:
        effective_udp_ip = (
            body.udp_server_ip
            if body.udp_server_ip is not None
            else current_settings.get("udp_server_ip")
        )
        effective_udp_port = (
            body.udp_server_port
            if body.udp_server_port is not None
            else current_settings.get("udp_server_port")
        )

        udp_ip = _validated_udp_ip(effective_udp_ip)
        udp_port = _validated_udp_port(effective_udp_port)

        repo.set_setting(db, "udp_server_ip", udp_ip)
        repo.set_setting(db, "udp_server_port", udp_port)
        patch["udp_server_ip"] = udp_ip
        patch["udp_server_port"] = udp_port

    if body.stop_after_current_batch is not None:
        enabled = bool(body.stop_after_current_batch)

        if enabled:
            st = repo.ensure_state_row(db)
            active_batch_id = st.active_batch_id

            if active_batch_id is None:
                active_batch_id = getattr(
                    st,
                    "active_operation_batch_id",
                    None,
                )

            if active_batch_id is None:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Останов после партии можно включить "
                        "только во время разбора активной партии"
                    ),
                )

            active_batch = repo.get_batch(
                db,
                int(active_batch_id),
            )

            if active_batch is None or str(active_batch.status) != "auto_processing":
                raise HTTPException(
                    status_code=409,
                    detail=("Текущая партия уже не находится " "в обработке"),
                )

            repo.set_setting(
                db,
                STOP_AFTER_BATCH_SETTING,
                int(active_batch_id),
            )
            patch["stop_after_current_batch"] = True
            patch["stop_after_batch_id"] = int(active_batch_id)
        else:
            repo.set_setting(
                db,
                STOP_AFTER_BATCH_SETTING,
                0,
            )
            patch["stop_after_current_batch"] = False
            patch["stop_after_batch_id"] = None

    if body.settings_ppod is not None:
        v = float(body.settings_ppod)
        if v < 0:
            v = 0.0
        if v > 1:
            v = 1.0
        repo.set_setting(db, "settings_ppod", v)
        patch["settings_ppod"] = v

    if body.consecutive_rejects_threshold is not None:
        threshold = int(body.consecutive_rejects_threshold)
        if threshold <= 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Количество браков подряд до проверки должно быть "
                    "целым числом больше 0"
                ),
            )
        repo.set_setting(
            db,
            "consecutive_rejects_threshold",
            threshold,
        )
        patch["consecutive_rejects_threshold"] = threshold

    if body.rjb_near_full is not None:
        u = int(body.rjb_near_full)
        repo.set_setting(db, "rjb_near_full", u)
        patch["rjb_near_full"] = u

    if body.temp_near_critical is not None:
        v = float(body.temp_near_critical)
        repo.set_setting(db, "temp_near_critical", v)
        patch["temp_near_critical"] = v

    if body.temperature_sensor_loading_min is not None:
        v = float(body.temperature_sensor_loading_min)
        repo.set_setting(db, "temperature_sensor_loading_min", v)
        patch["temperature_sensor_loading_min"] = v

    if body.temperature_sensor_loading_max is not None:
        v = float(body.temperature_sensor_loading_max)
        repo.set_setting(db, "temperature_sensor_loading_max", v)
        patch["temperature_sensor_loading_max"] = v

    if body.temperature_sensor_im_min is not None:
        v = float(body.temperature_sensor_im_min)
        repo.set_setting(db, "temperature_sensor_im_min", v)
        patch["temperature_sensor_im_min"] = v

    if body.temperature_sensor_im_max is not None:
        v = float(body.temperature_sensor_im_max)
        repo.set_setting(
            db, "temperature_sensor_im_max", float(body.temperature_sensor_im_max)
        )
        patch["temperature_sensor_im_max"] = v

    repo.add_user_audit_event(
        db,
        type_=EventType.SETTINGS_UPDATED.value,
        user_id=int(current_user.id),
        display_name=str(current_user.display_name),
        role=str(current_user.role),
        target={"type": "settings"},
        details={"changes": dict(patch)},
        legacy_payload={"by": by, **patch},
    )
    return {"ok": True, "patch": patch}


@router.get("/batches", response_model=list[BatchOut])
def list_batches(
    limit: int = 50,
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(select(BatchRow).order_by(BatchRow.id.desc()).limit(int(limit)))
        .scalars()
        .all()
    )

    return [_batch_out_from_row(batch) for batch in rows]


@router.post("/qr/scan")
async def scan_qr_code(
    request: Request,
    current_user: UserRow = Depends(require_current_user),
):
    if _qr_serial_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="QR-сканер уже ожидает считывание кода.",
        )

    async with _qr_serial_lock:
        raw = await _read_qr_serial_line(request)
        return {"data": _parse_qr_serial_payload(raw)}


@router.post("/batches", response_model=BatchOut)
def create_batch(
    body: BatchCreate,
    db: Session = Depends(get_db),
    current_user: UserRow = Depends(require_current_user),
):
    # Вся проверка выполняется до создания BatchRow и до постановки
    # команд открытия дверей. Ошибочная форма не меняет состояние системы.
    passport_number = _required_batch_text(
        body.passport_number,
        "Маршрутный паспорт",
    )
    passport_date = _normalize_passport_date(body.passport_date)
    product_code = _required_batch_text(
        body.product_code,
        "Обозначение детали",
    )
    product_name = _required_batch_text(
        body.product_name,
        "Наименование детали",
    )
    prod_fe = _required_batch_text(
        body.prod_fe,
        "Содержание железа",
    )
    blank_alloy = _required_batch_text(
        body.blank_alloy,
        "Сплав",
    )
    draw_rev = _required_batch_text(
        body.draw_rev,
        "Номер изменения чертежа",
    )
    blank_name = _required_batch_text(
        body.blank_name,
        "Заготовка",
    )
    cert_number = _required_batch_text(
        body.cert_number,
        "Номер сертификата",
    )
    prod_tsi = _required_batch_text(
        body.prod_tsi,
        "ТУ на слиток",
    )
    rod_batch_number = _required_batch_text(
        body.rod_batch_number,
        "Номер партии прутка",
    )
    prod_tsb = _required_batch_text(
        body.prod_tsb,
        "ТУ на заготовку",
    )

    try:
        items_mass = float(body.items_mass)
    except (TypeError, ValueError):
        items_mass = float("nan")

    if not math.isfinite(items_mass) or items_mass <= 0:
        raise HTTPException(
            status_code=422,
            detail="Масса должна быть числом больше нуля.",
        )

    batch_settings = repo.get_all_settings(db)

    try:
        layout = int(
            batch_settings.get(
                "layout",
                DEFAULT_BATCH_LAYOUT,
            )
        )
    except (TypeError, ValueError):
        layout = DEFAULT_BATCH_LAYOUT

    if layout not in (0, 1, 2, 3):
        layout = DEFAULT_BATCH_LAYOUT

    use_alternate_wave = bool(
        batch_settings.get(
            "use_alternate_wave",
            DEFAULT_USE_ALTERNATE_WAVE,
        )
    )

    try:
        product_rule = resolve_product_rule(
            product_code=product_code,
            layout=layout,
            use_alternate_wave=use_alternate_wave,
            product_count=body.product_count,
        )
    except ProductRuleError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        )

    is_special_product = uses_special_unloading_cell(product_code)

    loading_cell_no = int(body.cell_no)
    unloading_cell_no = int(
        SPECIAL_UNLOADING_CELL if is_special_product else loading_cell_no
    )

    if is_special_product and _unloading_cell_is_occupied(db, SPECIAL_UNLOADING_CELL):
        raise HTTPException(
            status_code=409,
            detail="Невозможно создать партию, ячейка 16 уже занята",
        )

    d = {
        # паспорт и введённые оператором данные партии
        "passport_number": passport_number,
        "passport_date": passport_date,
        "product_code": product_code,
        "product_name": product_name,
        "product_spec": int(product_rule.product_spec),
        "blank_alloy": blank_alloy,
        "blank_name": blank_name,
        "cert_number": cert_number,
        "rod_batch_number": rod_batch_number,
        "items_mass": items_mass,
        "prod_tsi": prod_tsi,
        "prod_tsb": prod_tsb,
        "draw_rev": draw_rev,
        "prod_fe": prod_fe,
        "prod_opt1": body.prod_opt1,
        "prod_opt2": body.prod_opt2,
        "prod_opt3": body.prod_opt3,
        "prod_opt4": body.prod_opt4,
        "prod_comment": body.prod_comment,
        "product_count": int(body.product_count),
        # счётчики измерений
        "measured_good": 0,
        "measured_bad": 0,
        "measured_qty": 0,
        "ok_qty": 0,
        "nok_qty": 0,
        # Загрузка и выгрузка теперь хранятся независимо.
        "in_tare_ids": [int(loading_cell_no)],
        "out_tare_ids": [int(unloading_cell_no)],
        # Партия может стать loaded только после того,
        # как supervisor действительно увидел обе двери
        # открытыми, а затем закрытыми.
        "loading_door_seen_open": False,
        "unloading_door_seen_open": False,
    }

    loc = {
        "cell_no": int(loading_cell_no),
        "in_tare_ids": [int(loading_cell_no)],
        "out_tare_ids": [int(unloading_cell_no)],
    }

    b = repo.create_batch(
        db,
        data=d,
        location=loc,
        status="blocked",
        source="WEB",
    )

    _enqueue_cmd(
        db,
        type_=CommandType.POSTAMAT_OPEN_LOADING_CELL.value,
        payload={
            "idx": int(loading_cell_no),
            "batch_id": int(b.id),
        },
        created_by=current_user.display_name,
    )

    _enqueue_cmd(
        db,
        type_=CommandType.POSTAMAT_OPEN_UNLOADING_CELL.value,
        payload={
            "idx": int(unloading_cell_no),
            "batch_id": int(b.id),
        },
        created_by=current_user.display_name,
    )

    db.refresh(b)
    return _batch_out_from_row(b)


@router.post("/batches/{batch_id}/open_cells")
def open_cells(
    batch_id: int,
    db: Session = Depends(get_db),
    current_user: UserRow = Depends(require_current_user),
):
    b = repo.get_batch(db, int(batch_id))
    if not b:
        raise HTTPException(
            status_code=404,
            detail=f"Партия {int(batch_id)} не найдена",
        )

    if b.status not in {"done", "rejected"}:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Извлечение партии {int(batch_id)} запрещено: "
                f"текущий статус {b.status}"
            ),
        )

    data = dict(b.data or {})
    if data.get("extraction_ready") is False:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Извлечение партии {int(batch_id)} "
                "временно запрещено: роботы ещё завершают "
                "безопасную остановку"
            ),
        )

    in_ids, out_ids = _tare_ids_from_batch(b)
    command_ids: list[int] = []

    for idx in in_ids:
        command = _enqueue_cmd(
            db,
            type_=CommandType.POSTAMAT_OPEN_LOADING_CELL.value,
            payload={
                "idx": int(idx),
                "batch_id": int(batch_id),
                "purpose": "extraction",
            },
            created_by=current_user.display_name,
        )
        command_ids.append(int(command.id))

    for idx in out_ids:
        command = _enqueue_cmd(
            db,
            type_=CommandType.POSTAMAT_OPEN_UNLOADING_CELL.value,
            payload={
                "idx": int(idx),
                "batch_id": int(batch_id),
                "purpose": "extraction",
            },
            created_by=current_user.display_name,
        )
        command_ids.append(int(command.id))

    return {
        "ok": True,
        "batch_id": int(batch_id),
        "in_tare_ids": in_ids,
        "out_tare_ids": out_ids,
        "command_ids": command_ids,
    }


@router.post("/batches/{batch_id}/activate")
def activate_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    current_user: UserRow = Depends(require_current_user),
):
    _enqueue_cmd(
        db,
        type_=CommandType.SET_ACTIVE_BATCH.value,
        payload={"batch_id": int(batch_id)},
        created_by=current_user.display_name,
    )
    return {"ok": True, "batch_id": int(batch_id)}


@router.post("/commands", response_model=CommandOut)
def create_command(
    body: CommandCreateLoose,
    db: Session = Depends(get_db),
    current_user: UserRow = Depends(require_current_user),
):
    cmd_type = _authorize_web_command(
        db,
        command_type=body.type,
        current_user=current_user,
    )

    cmd = _enqueue_cmd(
        db,
        type_=cmd_type,
        payload=(body.payload or {}),
        created_by=current_user.display_name,
    )

    return CommandOut(
        id=cmd.id,
        created_at=cmd.created_at,
        created_by=cmd.created_by,
        type=cmd.type,
        status=cmd.status,
        payload=cmd.payload,
        error=cmd.error,
    )


@router.get("/commands/{command_id}", response_model=CommandOut)
def get_command(
    command_id: int,
    db: Session = Depends(get_db),
):
    cmd = db.get(CommandRow, int(command_id))

    if cmd is None:
        raise HTTPException(
            status_code=404,
            detail=f"Команда {int(command_id)} не найдена",
        )

    return CommandOut(
        id=cmd.id,
        created_at=cmd.created_at,
        created_by=cmd.created_by,
        type=cmd.type,
        status=cmd.status,
        payload=cmd.payload,
        error=cmd.error,
    )


@router.get("/state", response_model=SystemState)
def get_state(db: Session = Depends(get_db)):
    row = repo.ensure_state_row(db)

    operator_messages = repo.list_operator_messages(db)

    last_summary = None
    if row.last_meas_summary:
        try:
            last_summary = json.loads(row.last_meas_summary)
        except Exception:
            last_summary = None

    return SystemState(
        updated_at=row.updated_at,
        mode=row.mode,
        safety_ok=bool(row.safety_ok),
        reject_bin_count=row.reject_bin_count,
        reject_bin_capacity=row.reject_bin_capacity,
        active_batch_id=row.active_batch_id,
        active_operation_id=getattr(row, "active_operation_id", None),
        active_operation_batch_id=getattr(row, "active_operation_batch_id", None),
        active_operation_phase=getattr(row, "active_operation_phase", None),
        active_operation_started_at=getattr(row, "active_operation_started_at", None),
        message=row.message,
        message_severity=str(
            repo.get_setting(db, "message_severity", Severity.info.value)
            or Severity.info.value
        ),
        operator_messages=operator_messages,
        trash_present=bool(row.trash_present),
        air_pressure_ok=bool(repo.get_setting(db, "air_pressure_ok", True)),
        rtk_connected=bool(row.rtk_connected),
        rtk_busy=bool(row.rtk_busy),
        rtk_state=row.rtk_state,
        rtk_action_r1=row.rtk_action_r1,
        rtk_action_r2=row.rtk_action_r2,
        rtk_error=row.rtk_error,
        rtk_manual_mode=bool(repo.get_setting(db, "rtk_manual_mode", False)),
        need_cycle_on=bool(repo.get_setting(db, "rtk_need_cycle_on", False)),
        pending_mm_result=(
            None if row.pending_mm_result is None else bool(row.pending_mm_result)
        ),
        mm_result_inflight=bool(row.mm_result_inflight),
        pending_calib_result=row.pending_calib_result,
        calib_result_inflight=bool(row.calib_result_inflight),
        consecutive_rejects=int(row.consecutive_rejects or 0),
        last_meas_ok=row.last_meas_ok,
        last_meas_not_ok=row.last_meas_not_ok,
        last_meas_summary=last_summary,
        calibration_inflight=bool(row.calibration_inflight),
        calibration_reason=row.calibration_reason,
    )


@router.get("/reject-bin/replacement-status")
def get_reject_bin_replacement_status(
    db: Session = Depends(get_db),
):
    st = repo.ensure_state_row(db)

    raw_tare_no = repo.get_setting(
        db,
        "rejectbin_replacement_tare_no",
        0,
    )

    try:
        tare_no = int(raw_tare_no)
    except (TypeError, ValueError):
        tare_no = 0

    tare_no_valid = 1 <= tare_no <= 10
    replacement_active = str(st.mode) == "paused_rejectbin"

    return {
        "replacement_active": replacement_active,
        "tare_no_required": (replacement_active and not tare_no_valid),
        "tare_no": tare_no if tare_no_valid else None,
    }


def _event_out_from_row(ev: EventRow) -> EventOut:
    return EventOut(
        id=ev.id,
        ts=ev.ts,
        severity=ev.severity,
        source=ev.source,
        type=ev.type,
        payload=ev.payload,
    )


@router.get("/events/history", response_model=list[EventOut])
def event_history(
    before_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Возвращает ограниченную страницу истории событий."""
    query = select(EventRow)

    if before_id is not None:
        query = query.where(EventRow.id < int(before_id))

    rows = (
        db.execute(query.order_by(EventRow.id.desc()).limit(int(limit))).scalars().all()
    )

    # UI отображает журнал в прямом хронологическом порядке.
    return [_event_out_from_row(ev) for ev in reversed(rows)]


@router.get("/events")
def sse_events(
    request: Request,
    after_id: int | None = Query(default=None, ge=0),
):
    """
    Live SSE-поток.

    Первое подключение без after_id начинается с текущего конца журнала.
    При реконнекте браузер передаёт Last-Event-ID, поэтому пропущенные
    live-события будут дочитаны без повторной выгрузки всей истории.
    """

    async def gen():
        last_hdr = request.headers.get("Last-Event-ID")
        last_from_hdr = None

        if last_hdr:
            try:
                parsed = int(last_hdr)
                if parsed >= 0:
                    last_from_hdr = parsed
            except (TypeError, ValueError):
                last_from_hdr = None

        if last_from_hdr is not None:
            last = last_from_hdr
        elif after_id is not None:
            last = int(after_id)
        else:
            # Новая вкладка получает только будущие события.
            db = SessionLocal()
            try:
                last = int(
                    db.execute(select(func.max(EventRow.id))).scalar_one_or_none() or 0
                )
            finally:
                db.close()

        last_send = time.monotonic()

        # Совет браузеру: при обрыве переподключаться через 1500 мс.
        yield "retry: 1500\n\n"

        while True:
            if await request.is_disconnected():
                break

            db = SessionLocal()
            try:
                rows = (
                    db.execute(
                        select(EventRow)
                        .where(EventRow.id > last)
                        .order_by(EventRow.id.asc())
                        .limit(100)
                    )
                    .scalars()
                    .all()
                )
            finally:
                db.close()

            if rows:
                for ev in rows:
                    data = _event_out_from_row(ev).model_dump(mode="json")

                    last = int(ev.id)
                    last_send = time.monotonic()

                    yield (
                        f"id: {ev.id}\n"
                        "event: message\n"
                        f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                    )

                # Если после реконнекта накопилось несколько страниц,
                # дочитываем их сразу, без искусственной паузы.
                continue

            if time.monotonic() - last_send >= 10.0:
                last_send = time.monotonic()
                yield ": ping\n\n"

            await asyncio.sleep(0.3)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers=headers,
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(
    current_user: UserRow | None = Depends(get_current_user_optional),
):
    if current_user is not None:
        return RedirectResponse(url="/api/workplace", status_code=303)

    return HTMLResponse(r"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Вход — Постаматы</title>
  <style>
    :root{
      font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      color:#273142;
      background:#f6f7f9;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      min-height:100vh;
      display:flex;
      align-items:center;
      justify-content:center;
      padding:16px;
      background:#f6f7f9;
    }
    .loginCard{
      width:min(440px,100%);
      background:#fff;
      border:1px solid #d9dde5;
      border-radius:10px;
      padding:20px;
      box-shadow:0 8px 30px rgba(0,0,0,.06);
    }
    h1{margin:0 0 18px;font-size:24px}
    .tabs{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:6px;
      margin-bottom:18px;
    }
    .tab{
      min-height:38px;
      border:1px solid #cfd5df;
      border-radius:7px;
      background:#fff;
      font-weight:700;
      cursor:pointer;
    }
    .tab.active{
      background:#eef4ff;
      border-color:#7aa2e3;
      color:#174a8b;
    }
    .panel.hidden{display:none}
    .field{display:block;margin-bottom:12px}
    .label{
      display:block;
      margin-bottom:5px;
      font-size:13px;
      font-weight:700;
      color:#4b5563;
    }
    input,select{
      width:100%;
      min-height:40px;
      border:1px solid #cfd5df;
      border-radius:7px;
      padding:7px 9px;
      background:#fff;
      color:#273142;
      font:inherit;
    }
    .submitBtn{
      width:100%;
      min-height:42px;
      margin-top:4px;
      border:1px solid #245da8;
      border-radius:7px;
      background:#2f6db7;
      color:#fff;
      font:inherit;
      font-weight:700;
      cursor:pointer;
    }
    .submitBtn:disabled{opacity:.55;cursor:not-allowed}
    .message{
      min-height:20px;
      margin-top:12px;
      font-size:13px;
      color:#6b7280;
    }
    .message.error{color:#b42318}
    .message.ok{color:#166534}
  </style>
</head>
<body>
  <main class="loginCard">
    <h1>Вход в систему</h1>

    <div class="tabs" role="tablist" aria-label="Тип входа">
      <button id="operatorTab" class="tab active" type="button" onclick="showMode('operator')">
        Оператор
      </button>
      <button id="adminTab" class="tab" type="button" onclick="showMode('admin')">
        Администратор
      </button>
    </div>

    <form id="operatorPanel" class="panel" onsubmit="loginOperator(event)">
      <label class="field">
        <span class="label">Пользователь</span>
        <select id="operatorSelect" required>
          <option value="">Загрузка…</option>
        </select>
      </label>
      <label class="field">
        <span class="label">Пароль</span>
        <input id="operatorPassword" type="password" maxlength="1024" autocomplete="current-password" required />
      </label>
      <button id="operatorSubmit" class="submitBtn" type="submit">Войти</button>
    </form>

    <form id="adminPanel" class="panel hidden" onsubmit="loginAdmin(event)">
      <label class="field">
        <span class="label">Логин</span>
        <input id="adminLogin" type="text" maxlength="128" autocomplete="username" required />
      </label>
      <label class="field">
        <span class="label">Пароль</span>
        <input id="adminPassword" type="password" maxlength="1024" autocomplete="current-password" required />
      </label>
      <button id="adminSubmit" class="submitBtn" type="submit">Войти</button>
    </form>

    <div id="loginMessage" class="message" aria-live="polite"></div>
  </main>

<script>
  const API_BASE = location.pathname.replace(/\/login\/?$/, '');

  function setMessage(text, kind=''){
    const el = document.getElementById('loginMessage');
    el.className = `message ${kind}`.trim();
    el.textContent = text || '';
  }

  function showMode(mode){
    const operator = mode !== 'admin';
    document.getElementById('operatorPanel').classList.toggle('hidden', !operator);
    document.getElementById('adminPanel').classList.toggle('hidden', operator);
    document.getElementById('operatorTab').classList.toggle('active', operator);
    document.getElementById('adminTab').classList.toggle('active', !operator);
    setMessage('');
    setTimeout(() => {
      (operator
        ? document.getElementById('operatorSelect')
        : document.getElementById('adminLogin')
      )?.focus();
    }, 0);
  }

  async function fetchJson(url, options){
    const response = await fetch(url, options);
    const text = await response.text();
    let body;
    try{ body = text ? JSON.parse(text) : null; }catch(e){ body = text; }
    if(!response.ok){
      const message = (
        body && typeof body.detail === 'string'
          ? body.detail
          : typeof body === 'string' && body.trim()
          ? body.trim()
          : `HTTP ${response.status}`
      );
      throw new Error(message);
    }
    return body;
  }

  async function loadOperators(){
    const select = document.getElementById('operatorSelect');
    const submit = document.getElementById('operatorSubmit');
    try{
      const operators = await fetchJson(`${API_BASE}/auth/operators`);
      select.innerHTML = '';
      if(!Array.isArray(operators) || operators.length === 0){
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'Нет доступных операторов';
        select.appendChild(option);
        select.disabled = true;
        submit.disabled = true;
        return;
      }
      const placeholder = document.createElement('option');
      placeholder.value = '';
      placeholder.textContent = 'Выберите пользователя';
      select.appendChild(placeholder);
      for(const operator of operators){
        const option = document.createElement('option');
        option.value = String(operator?.id || '');
        option.textContent = String(operator?.display_name || '—');
        select.appendChild(option);
      }
      select.disabled = false;
      submit.disabled = false;
    }catch(e){
      select.innerHTML = '<option value="">Ошибка загрузки пользователей</option>';
      select.disabled = true;
      submit.disabled = true;
      setMessage(`Ошибка: ${e?.message || String(e)}`, 'error');
    }
  }

  async function loginOperator(event){
    event.preventDefault();
    const userId = Number(document.getElementById('operatorSelect').value || 0);
    const password = document.getElementById('operatorPassword').value || '';
    if(!userId){ setMessage('Выберите пользователя.', 'error'); return; }
    if(!password){ setMessage('Введите пароль.', 'error'); return; }
    setMessage('Вход…');
    try{
      await fetchJson(`${API_BASE}/auth/login/operator`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({user_id:userId,password})
      });
      location.replace(`${API_BASE}/workplace`);
    }catch(e){
      document.getElementById('operatorPassword').value = '';
      setMessage(e?.message || 'Не удалось войти.', 'error');
    }
  }

  async function loginAdmin(event){
    event.preventDefault();
    const login = document.getElementById('adminLogin').value.trim();
    const password = document.getElementById('adminPassword').value || '';
    if(!login){ setMessage('Введите логин.', 'error'); return; }
    if(!password){ setMessage('Введите пароль.', 'error'); return; }
    setMessage('Вход…');
    try{
      await fetchJson(`${API_BASE}/auth/login/admin`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({login,password})
      });
      location.replace(`${API_BASE}/workplace`);
    }catch(e){
      document.getElementById('adminPassword').value = '';
      setMessage(e?.message || 'Не удалось войти.', 'error');
    }
  }

  loadOperators();
</script>
</body>
</html>
        """.strip())


@router.get("/workplace", response_class=HTMLResponse)
def workplace(
    current_user: UserRow | None = Depends(get_current_user_optional),
):
    if current_user is None:
        return RedirectResponse(url="/api/login", status_code=303)

    return HTMLResponse(r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Рабочее место</title>
  <style>
    body{font-family:system-ui;margin:16px}
    .top{display:flex;flex-direction:column;gap:8px;align-items:stretch;margin-bottom:12px;}
    .topRow{display:flex;gap:12px;align-items:center;flex-wrap:nowrap;min-height:32px;}
    .paramsRow > div{white-space:nowrap;}
    .controlsRow{gap:8px;flex-wrap:wrap;}
    .controlsLabel{font-weight:700;margin-right:4px;white-space:nowrap;}
    .stopAfterOption{
      display:inline-flex;
      align-items:center;
      gap:6px;
      white-space:nowrap;
      font-size:13px;
      font-weight:600;
      cursor:pointer;
    }
    .stopAfterOption.disabled{
      color:#9ca3af;
      cursor:not-allowed;
    }
    .stopAfterOption input{
      width:18px;
      height:18px;
      margin:0;
    }
    .messagesPanel{
      display:flex;
      flex-direction:column;
      gap:0;
      padding:0 12px;
      box-sizing:border-box;
    }
    .messagesPanel.empty{display:none;}
    .operatorMessage{
      display:flex;
      align-items:flex-start;
      gap:5px;
      min-width:0;
      padding:2px 6px;
      border-radius:0;
      box-sizing:border-box;
      font-size:13px;
      font-weight:600;
      line-height:1.2;
      overflow-wrap:anywhere;
    }
    .operatorMessage + .operatorMessage{border-top-width:0;}
    .operatorMessage.info{color:#075985;background:#e0f2fe;border:1px solid #bae6fd;}
    .operatorMessage.warn{color:#9a3412;background:#ffedd5;border:1px solid #fed7aa;}
    .operatorMessage.error{color:#991b1b;background:#fee2e2;border:1px solid #fecaca;}
    .operatorMessageIcon{
      flex:0 0 auto;
      width:14px;
      text-align:center;
      font-size:12px;
      line-height:1.2;
    }
    .operatorMessageText{
      min-width:0;
      flex:1 1 auto;
    }
    .ok{color:#0a0;font-weight:700}
    .bad{color:#b00;font-weight:700}
    .grid{display:grid;grid-template-columns:1fr 280px;gap:10px}
    .card{border:1px solid #ddd;border-radius:12px;padding:12px}
    .btn{padding:8px 10px;border:1px solid #aaa;border-radius:10px;background:#fff;cursor:pointer}
    .btn.sm{padding:5px 8px;border-radius:8px;font-size:12px}
    .btn:disabled{opacity:.45;cursor:not-allowed;background:#f3f3f3}
    .btn.cycleOnTrue{color:#166534;background:#dcfce7;border-color:#86efac;font-weight:700;}
    .btn.cycleOnFalse{color:#991b1b;background:#fee2e2;border-color:#fca5a5;font-weight:700;}
    .btn.soundIconBtn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      width:26px;
      height:23px;
      min-width:26px;
      padding:0;
      border-radius:0;
      font-size:16px;
      line-height:1;
    }
    .btn.soundIconBtn.soundMuted{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
      font-weight:700;
    }
    small{color:#666}
    h3{margin:0 0 10px 0}
    .muted{color:#777}
    .pill{display:inline-block;padding:2px 8px;border:1px solid #ddd;border-radius:999px;font-size:12px}
    .cells{display:grid;grid-template-columns:repeat(8, minmax(0, 1fr));gap:8px}
    .cell{border:1px solid #e5e5e5;border-radius:12px;padding:8px;min-height:86px;display:flex;flex-direction:column;justify-content:space-between}
    .cell .n{font-weight:800}
    .cell .meta{font-size:12px;color:#555;line-height:1.2}
    .cell .actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}
    .cell.busy{border-color:#bbb;background:#fafafa}
    .cell.loaded{border-color:#b7d7ff;background:#f4f9ff}
    .cell.auto{border-color:#c9f1c9;background:#f6fff6}
    .cell.done{border-color:#eee;background:#fff}
    .cell.blocked{border-color:#ffd2d2;background:#fff6f6}
    .rowline{display:flex;gap:8px;align-items:center;justify-content:space-between;border-bottom:1px solid #eee;padding:8px 0}
    .norowline{display:flex;gap:8px;align-items:center;justify-content:space-between;padding:8px 0}
    .measrow{display:grid;grid-template-columns:1fr 110px 70px;gap:10px;align-items:center;border-bottom:1px solid #eee;padding:8px 0}
    .measrow:last-child{border-bottom:none}
    .measrow .v{text-align:right}
    .measrow .s{text-align:right}
    .mono{font-family:ui-monospace, SFMono-Regular, Menlo, monospace;font-size:12px}
    .inp{width:100%;padding:6px;border:1px solid #ccc;border-radius:10px;box-sizing:border-box;max-width:100%;}
    .processSelect{
      padding:8px 10px;
      border:1px solid #aaa;
      border-radius:10px;
      background:#fff;
      box-sizing:border-box;
    }

    .manualModeLabel{
      white-space:nowrap;
      font-size:13px;
      font-weight:600;
      margin-left:4px;
    }    
    .field{display:flex;flex-direction:column;gap:4px}
    .fields{display:flex;flex-direction:column;gap:8px}
    
    .nbTitleControls{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
    .nbTitleControls b{white-space:nowrap}

    .waveOption{
      display:inline-flex;
      align-items:center;
      gap:6px;
      white-space:nowrap;
      font-size:13px;
      font-weight:600;
    }

    .waveOption input{
      width:18px;
      height:18px;
      margin:0;
    }

    .batchFormGrid{
      display:grid;
      grid-template-columns:repeat(3, 25ch);
      gap:8px 12px;
      align-items:end;
      width:max-content;
      max-width:100%;
    }

    .batchFormGrid .field{min-width:0}
    .batchFormGrid .inp{width:100%;max-width:100%}
    .batchFormGrid .fieldWide{grid-column:1 / -1}
    .batchFormGrid .fieldWide .inp{width:100%}

    .modal{position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:flex-start;justify-content:center;padding:18px;z-index:9999}
    .modalCard{background:#fff;border-radius:14px;max-width:520px;width:100%;padding:12px;box-shadow:0 12px 40px rgba(0,0,0,.2);box-sizing:border-box;overflow-x:hidden;}
    #nbModal .modalCard{
      width:max-content;
      max-width:calc(100vw - 36px);
    }
    .modalHead{display:flex;align-items:center;justify-content:space-between;gap:10px}
    .hidden{display:none !important}
    .qrCapture{
      position:fixed;
      left:-10000px;
      top:-10000px;
      width:1px;
      height:1px;
      opacity:.01;
      resize:none;
    }

    .qrStatus{
      min-height:20px;
      margin-top:8px;
      padding:7px 9px;
      border-radius:9px;
      box-sizing:border-box;
      font-size:13px;
    }

    .qrStatus.empty{
      margin-top:0;
      padding:0;
      min-height:0;
    }

    .qrStatus.waiting{
      color:#075985;
      background:#e0f2fe;
      border:1px solid #bae6fd;
    }

    .qrStatus.ok{
      color:#166534;
      background:#dcfce7;
      border:1px solid #86efac;
    }

    .qrStatus.error{
      color:#991b1b;
      background:#fee2e2;
      border:1px solid #fecaca;
    }

    .btn.qrWaiting{
      color:#075985;
      background:#e0f2fe;
      border-color:#7dd3fc;
      font-weight:700;
    }

    /* ---------- Workplace layout v2 ---------- */

    body{
      margin:0;
      color:#273142;
      background:#fff;
    }

    .top{
      gap:0;
      margin-bottom:10px;
      background:#fff;
    }

    .topRow{
      min-height:48px;
      padding:8px 12px;
      box-sizing:border-box;
      border-bottom:1px solid #e5e7eb;
    }

    .headerRow{
      justify-content:space-between;
      flex-wrap:wrap;
      gap:10px;
    }

    .headerLeft,
    .headerRight,
    .rtkEquipmentGroup{
      display:flex;
      align-items:center;
      gap:4px;
      flex-wrap:wrap;
    }

    .clockText{
      min-width:153px;
      font-family:ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size:13px;
      color:#374151;
      white-space:nowrap;
    }

    .statusField{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-height:23px;
      padding:1px 3px;
      box-sizing:border-box;
      border:1px solid #d1d5db;
      border-radius:0;
      font-size:13px;
      font-weight:600;
      white-space:nowrap;
      transition:
        background-color .15s ease,
        border-color .15s ease,
        color .15s ease;
    }

    .statusField.statusOk{
      color:#166534;
      background:#dcfce7;
      border-color:#86efac;
    }

    .statusField.statusBad{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
    }

    .statusField.statusPaused{
      color:#9a3412;
      background:#ffedd5;
      border-color:#fdba74;
    }

    .statusField.statusUnknown{
      color:#6b7280;
      background:#f3f4f6;
      border-color:#d1d5db;
    }

    .separator{
      color:#d1d5db;
      user-select:none;
    }

    .tareNumber{
      display:inline-flex;
      align-items:center;
      gap:2px;
      color:#4b5563;
      white-space:nowrap;
    }

    .rejectFill{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-width:52px;
      min-height:23px;
      padding:1px 3px;
      box-sizing:border-box;
      border:1px solid #d1d5db;
      border-radius:0;
      font-family:ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size:13px;
      font-weight:700;
      white-space:nowrap;
    }

    .rejectFill.rejectNormal{
      color:#166534;
      background:#dcfce7;
      border-color:#86efac;
    }

    .rejectFill.rejectNear{
      color:#9a3412;
      background:#ffedd5;
      border-color:#fdba74;
    }

    .rejectFill.rejectFull{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
    }

    .rejectFill.rejectUnknown{
      color:#6b7280;
      background:#f3f4f6;
      border-color:#d1d5db;
    }

    .temperatureField{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-height:23px;
      padding:1px 3px;
      box-sizing:border-box;
      border:1px solid #d1d5db;
      border-radius:0;
      font-family:ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size:13px;
      font-weight:700;      
      white-space:nowrap;
    }

    .temperatureField.tempOk{
      color:#166534;
      background:#dcfce7;
      border-color:#86efac;
    }

    .temperatureField.tempNear{
      color:#9a3412;
      background:#ffedd5;
      border-color:#fdba74;
    }

    .temperatureField.tempBad{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
    }

    .temperatureField.tempUnknown{
      color:#6b7280;
      background:#f3f4f6;
      border-color:#d1d5db;
    }

    .userArea{
      display:flex;
      align-items:center;
      gap:8px;
      color:#6b7280;
      font-size:13px;
      white-space:nowrap;
    }

    .userNameLink{
      color:#374151;
      font-weight:700;
      text-decoration:none;
      cursor:default;
    }

    .userNameLink.admin{
      color:#174a8b;
      cursor:pointer;
      text-decoration:underline;
      text-underline-offset:2px;
    }

    .logoutBtn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      width:30px;
      height:30px;
      padding:0;
      border:0;
      background:transparent;
      color:#000;
    }

    .logoutBtn:disabled{
      opacity:.55;
      cursor:not-allowed;
    }

    .rtkRow{
      gap:9px;
      flex-wrap:wrap;
    }

    .controlsRow{
      gap:5px;
      flex-wrap:wrap;
    }

    .controlsLabel{
      margin-right:0;
      color:#4b5563;
      font-size:16px;
      font-weight:700;
    }

    #rtk,
    #imConnection{
      font-size:16px;
      font-weight:700;
    }

    .grid{
      grid-template-columns:minmax(0, 1fr) 210px;
      gap:4px;
      padding:0 4px 4px;
      box-sizing:border-box;
    }

    .lockersColumn{
      display:flex;
      flex-direction:column;
      gap:6px;
      min-width:0;
    }

    .card{
      border-color:#e5e7eb;
      box-shadow:0 1px 2px rgba(15, 23, 42, .03);
    }

    .lockerCard{
      padding:4px;
    }

    .sectionTitle{
      padding:2px 6px 4px;
      font-size:15px;
      font-weight:800;
      text-transform:uppercase;
      color:#4b5563;
    }

    .cells{
      gap:4px;
    }

    .cell{
      min-height:112px;
      padding:9px;
      box-sizing:border-box;
      border-width:2px;
      border-radius:13px;
      background:#fff;
    }

    .cellHead{
      display:flex;
      align-items:flex-start;
      justify-content:space-between;
      gap:5px;
      margin-bottom:7px;
    }

    .cell .n{
      color:#1f2937;
      white-space:nowrap;
    }

    .cell .meta{
      color:#6b7280;
      font-size:12px;
      line-height:1.35;
      overflow-wrap:anywhere;
    }

    .cell .pill{
      padding:2px 6px;
      border-radius:6px;
      font-weight:700;
      line-height:1.2;
      white-space:nowrap;
    }

    .cell.auto{
      border-color:#22c55e;
      background:#f0fdf4;
    }

    .cell.auto .pill{
      color:#166534;
      background:#dcfce7;
      border-color:#86efac;
    }

    .cell.loaded{
      border-color:#60a5fa;
      background:#eff6ff;
    }

    .cell.loaded .pill{
      color:#1d4ed8;
      background:#dbeafe;
      border-color:#93c5fd;
    }

    .cell.rejected{
      border-color:#f59e0b;
      background:#fff7ed;
    }

    .cell.rejected .pill{
      color:#9a3412;
      background:#ffedd5;
      border-color:#fdba74;
    }

    .cell.done{
      border-color:#86efac;
      background:#f7fee7;
    }

    .cell.done .pill{
      color:#15803d;
      background:#dcfce7;
      border-color:#86efac;
    }

    .cell.blocked{
      border-color:#9ca3af;
      background:#f3f4f6;
    }

    .cell.blocked .pill{
      color:#4b5563;
      background:#e5e7eb;
      border-color:#9ca3af;
    }

    .cell.busy{
      border-color:#cbd5e1;
      background:#f8fafc;
    }

    .activeBatchCard{
      align-self:stretch;
      height:100%;
      min-height:0;
      max-height:100%;
      overflow:auto;
      box-sizing:border-box;
    }

    .activeBatchHead{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      padding:2px 0 6px;
      border-bottom:1px solid #eee;
      font-size:15px;
      font-weight:800;
    }

    .activeBatchHead b{
      text-align:right;
      overflow-wrap:anywhere;
    }

    .rowline{
      padding:9px 0;
      color:#6b7280;
      font-size:13px;
    }

    .rowline b{
      color:#374151;
    }

    .badMeasField{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:8px;
      margin-top:7px;
      padding:8px 9px;
      border:1px solid #fecaca;
      border-radius:7px;
      color:#7f1d1d;
      background:#fef2f2;
      box-sizing:border-box;
    }

    .badMeasField b{
      color:#450a0a;
      text-align:right;
    }

    @media (max-width: 900px){
      .grid{
        grid-template-columns:1fr;
      }

      .activeBatchCard{
        height:auto;
        min-height:0;
        max-height:none;
        overflow:visible;
      }

      .cells{
        grid-template-columns:repeat(4, minmax(0, 1fr));
      }
    }

    @media (max-width: 620px){
      .cells{
        grid-template-columns:repeat(2, minmax(0, 1fr));
      }

      .clockText{
        min-width:0;
        width:100%;
      }
    }

    @media (max-width: 840px){
      #nbModal .modalCard{width:100%;max-width:520px}
      .batchFormGrid{grid-template-columns:1fr;width:100%}
      .batchFormGrid .inp{width:100%}
    }        
  </style>
</head>
<body>
  <div class="top">
    <div class="topRow headerRow">
      <div class="headerLeft">
        <div id="currentDateTime" class="clockText">
          --.--.---- --:--:--
        </div>

        <span
          id="safety"
          class="statusField statusUnknown"
        >
          АвКонтур
        </span>

        <span class="separator">|</span>

        <span
          id="air_pressure"
          class="statusField statusUnknown"
        >
          Воздух
        </span>

        <span class="separator">|</span>

        <span
          id="trash"
          class="statusField statusUnknown"
        >
          Тара брака
        </span>

        <span class="tareNumber">
          №<b id="reject_tare_no">-</b>
        </span>

        <span
          id="reject"
          class="rejectFill rejectUnknown"
        >
          -
        </span>

        <span class="separator">|</span>        

        <span
          id="temp_im_field"
          class="temperatureField tempUnknown"
        >
          tм:&nbsp;<b id="temp_im">-</b><span id="temp_im_unit"></span>
        </span>

        <span
          id="temp_loading_field"
          class="temperatureField tempUnknown"
        >
          tп:&nbsp;<b id="temp_loading">-</b><span id="temp_loading_unit"></span>
        </span>

        <span id="emergencySoundSeparator" class="separator hidden">|</span>

        <button
          id="emergencySoundBtn"
          class="btn soundIconBtn hidden"
          type="button"
          onclick="toggleEmergencySound()"
          aria-pressed="false"
          aria-label="Отключить аварийный звук"
          title="Аварийный звук сейчас не активен"
          disabled
        >
          🔊
        </button>
      </div>

      <div class="headerRight">
        <div class="userArea">
          <a id="workplaceUser" class="userNameLink">—</a>

          <button
            class="logoutBtn"
            type="button"
            onclick="logoutUser()"
            title="Выйти"
            aria-label="Выйти из системы"
          >
            <svg
              width="18"
              height="18"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.8"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true"
            >
              <path d="M10 17l5-5-5-5"/>
              <path d="M15 12H3"/>
              <path d="M14 3h5a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-5"/>
            </svg>
          </button>
        </div>
      </div>
    </div>

    <div class="topRow rtkRow">
      <div class="rtkEquipmentGroup">
        <span
          id="rtk"
          class="statusField statusUnknown"
        >
          РТК:
        </span>

        <button
          id="cycleOnBtn"
          class="btn"
          type="button"
          onclick="cycleOn()"
        >
          Включить РТК
        </button>

        <span class="separator">|</span>

        <span
          id="robotR2"
          class="statusField statusUnknown"
        >
          RS007L
        </span>

        <span class="separator">|</span>

        <span
          id="robotR1"
          class="statusField statusUnknown"
        >
          RS013N
        </span>

        <span class="separator">|</span>

        <span class="muted">Режим:</span>

        <select
          id="rtkModeSelect"
          class="processSelect"
          onchange="changeRtkMode()"
        >
          <option value="auto">Автоматический</option>
          <option value="manual">Ручной</option>
        </select>

        <button
          id="rtkNextStepBtn"
          class="btn"
          type="button"
          onclick="rtkNextStep()"
          disabled
        >
          Следующий шаг
        </button>

        <span class="separator">|</span>

        <span
          id="imConnection"
          class="statusField statusUnknown"
        >
          Микрометр:
        </span>

        <span class="muted"><b id="imLoadedProgram">-</b></span>

      </div>

      <!-- Оставляем служебные элементы для существующего JS/SSE. -->
      <span id="mode" class="hidden">-</span>
      <span id="conn" class="hidden">SSE: -</span>
    </div>

    <div class="topRow controlsRow">
      <div class="controlsLabel">
        Автомат
      </div>

      <span
        id="autoModeStatus"
        class="statusField statusUnknown"
      >
        -
      </span>

      <span class="separator">|</span>

      <button
        id="startAutoBtn"
        class="btn"
        type="button"
        onclick="startAuto()"
      >
        СТАРТ
      </button>

      <button
        id="stopAutoBtn"
        class="btn"
        type="button"
        onclick="stopAuto()"
      >
        СТОП
      </button>

      <button
        id="pauseSysBtn"
        class="btn"
        type="button"
        onclick="pauseSys()"
      >
        ПАУЗА
      </button>

      <button
        class="btn"
        type="button"
        onclick="resumeSys()"
      >
        ПРОДОЛЖИТЬ
      </button>

      <button
        class="btn"
        type="button"
        onclick="replaceRB()"
      >
        Выгрузка брака
      </button>

      <span class="separator">|</span>

      <label
        id="stopAfterCurrentBatchLabel"
        class="stopAfterOption disabled"
        title="Останов можно включить во время разбора партии"
      >
        <span>Останов</span>
        <input
          id="stopAfterCurrentBatchCheckbox"
          type="checkbox"
          onchange="toggleStopAfterCurrentBatch()"
          disabled
        />
      </label>

      <span
        id="rtkResetSeparator"
        class="separator hidden"
      >|</span>

      <button
        id="rtkResetBtn"
        class="btn hidden"
        type="button"
        onclick="resetRtk()"
        disabled
      >
        СБРОС РТК
      </button>

    </div>

    <div id="msg" class="messagesPanel empty" aria-live="polite"></div>
  </div>

  <div class="grid">
    <div class="lockersColumn">
      <div class="card lockerCard">
        <div class="sectionTitle">Загрузка</div>
        <div id="cellsLoading" class="cells"></div>
      </div>

      <div class="card lockerCard">
        <div class="sectionTitle">Выгрузка</div>
        <div id="cellsUnloading" class="cells"></div>
      </div>
    </div>

    <div class="card activeBatchCard">
      <div class="activeBatchHead">
        <span>Партия</span>
        <b id="ab">-</b>
      </div>

      <div class="rowline">
        <div>Время разбора</div>
        <div><b id="ab_time">-</b></div>
      </div>

      <div class="rowline">
        <div>Разбраковано</div>
        <div><b id="ab_prog">-</b></div>
      </div>

      <div class="rowline">
        <div>Годные / Брак</div>
        <div><b id="lm_ok">-</b></div>
      </div>

      <div
        id="lm_items"
        class="muted"
        style="margin-top:10px"
      ></div>
    </div>
  </div>

<script>
let batchesCache = [];
let stateCache = null;
let es = null;
let currentBatch = null;
let extractionBusy = false;
let extractionStage = 'idle';
let extractionDocsPrinted = false;

let liveRefreshTimerId = null;
let liveRefreshInFlight = false;
let liveRefreshPending = false;
const LIVE_REFRESH_DEBOUNCE_MS = 100;

let tempsFetchedAtMs = 0;
let tempsFetchInFlight = null;

let emergencyActive = false;
let emergencySoundMuted = false;
let emergencySoundToggleInFlight = false;
let workplaceIsAdmin = false;
let rtkResetInFlight = false;

let rtkConnectedR1 = null;
let rtkConnectedR2 = null;

let imConnected = null;
let imLoadedProgram = '';

let rejectBinTareNo = null;
let rejectBinNearFull = 1;

let stopAfterCurrentBatch = false;
let stopAfterBatchId = null;
let stopAfterBatchToggleInFlight = false;

const LOADING_COUNT = 15;
const UNLOADING_COUNT = 16;
const SPECIAL_UNLOADING_PRODUCT_CODES = new Set([
  '312.229.001',
  '312.229.001-01',
  '312.229.001-02',
]);
const SPECIAL_UNLOADING_CELL = 16;

let newBatchCellNo = null;
let autoStartedAtMs = null;
let autoStartedBatchId = null;

let qrScanActive = false;
let qrScanAbortController = null;

let rejectBinModalManual = false;
let rejectBinSubmitInFlight = false;
let rejectBinStatusInFlight = false;

function v(id){ return (document.getElementById(id)?.value ?? '').trim(); }

function isPausedSystemMode(){
  return String(stateCache?.mode || '').startsWith('paused');
}

function renderStopAfterCurrentBatch(){
  const checkbox = document.getElementById(
    'stopAfterCurrentBatchCheckbox'
  );
  const label = document.getElementById(
    'stopAfterCurrentBatchLabel'
  );

  if(!checkbox || !label) return;

  const activeBatchId = Number(
    stateCache?.active_batch_id || 0
  );
  const hasActiveBatch = (
    Number.isInteger(activeBatchId) &&
    activeBatchId > 0
  );

  checkbox.checked = !!stopAfterCurrentBatch;
  checkbox.disabled = (
    !hasActiveBatch ||
    stopAfterBatchToggleInFlight
  );

  label.classList.toggle(
    'disabled',
    checkbox.disabled
  );

  if(stopAfterCurrentBatch && stopAfterBatchId){
    label.title = (
      `После завершения партии ${stopAfterBatchId} ` +
      'система перейдёт в idle'
    );
  }else if(hasActiveBatch){
    label.title = (
      `Остановить автоматический режим после партии ` +
      `${activeBatchId}`
    );
  }else{
    label.title = (
      'Останов можно включить во время разбора партии'
    );
  }
}

// QR-code reader functions
function setInputValue(id, value){
  const el = document.getElementById(id);
  if(!el) return;

  el.value = (
    value === null || value === undefined
      ? ''
      : String(value)
  );
}

function setQrStatus(message, kind='empty'){
  const el = document.getElementById('nb_qr_status');
  if(!el) return;

  const allowed = ['empty', 'waiting', 'ok', 'error'];
  const cls = allowed.includes(kind) ? kind : 'empty';

  el.className = `qrStatus ${cls}`;
  el.textContent = message || '';
}

function apiErrorMessage(error, fallback='Не удалось выполнить операцию'){
  if(typeof error === 'string'){
    return error.trim() || fallback;
  }

  if(error && typeof error.detail === 'string'){
    return error.detail.trim() || fallback;
  }

  /*
   * FastAPI иногда возвращает detail в виде массива,
   * например при ошибках валидации Pydantic.
   */
  if(error && Array.isArray(error.detail)){
    const messages = error.detail
      .map(item => {
        if(typeof item === 'string') return item;
        if(item && typeof item.msg === 'string') return item.msg;
        return '';
      })
      .filter(Boolean);

    if(messages.length > 0){
      return messages.join('; ');
    }
  }

  if(error && typeof error.message === 'string'){
    return error.message.trim() || fallback;
  }

  return fallback;
}

function resetQrScanButton(){
  const btn = document.getElementById('nb_qr_scan_btn');
  if(!btn) return;

  btn.classList.remove('qrWaiting');
  btn.textContent = 'Сканировать QR';
  btn.disabled = false;
}

function stopQrScan(message='', kind='empty'){
  qrScanActive = false;

  if(qrScanAbortController !== null){
    qrScanAbortController.abort();
    qrScanAbortController = null;
  }

  resetQrScanButton();

  setQrStatus(message, kind);
}

async function startQrScan(){
  const modal = document.getElementById('nbModal');
  const btn = document.getElementById('nb_qr_scan_btn');

  if(!modal || modal.classList.contains('hidden') || qrScanActive){
    return;
  }

  qrScanActive = true;
  const controller = new AbortController();
  qrScanAbortController = controller;

  if(btn){
    btn.classList.add('qrWaiting');
    btn.textContent = 'Ожидание QR...';
    btn.disabled = true;
  }

  setQrStatus(
    'Ожидание QR-кода с COM-сканера… Отсканируйте код.',
    'waiting'
  );

  try{
    const response = await api('qr/scan', {
      method:'POST',
      signal:controller.signal,
    });

    if(
      !qrScanActive ||
      qrScanAbortController !== controller
    ){
      return;
    }

    const data = response?.data;
    const normalized = validateQrBatchData(data);
    fillNewBatchFromQr(data, normalized);

    stopQrScan(
      'QR-код считан. Проверьте данные партии и нажмите «Создать партию».',
      'ok'
    );
  }catch(e){
    if(e?.name === 'AbortError') return;

    if(
      !qrScanActive ||
      qrScanAbortController !== controller
    ){
      return;
    }

    stopQrScan(
      `QR-код не принят: ${apiErrorMessage(e, 'ошибка чтения COM-сканера')}`,
      'error'
    );
  }
}

function isRealDate(year, month, day){
  const dt = new Date(Date.UTC(year, month - 1, day));

  return (
    dt.getUTCFullYear() === year &&
    dt.getUTCMonth() === month - 1 &&
    dt.getUTCDate() === day
  );
}

function qrDateToInputDate(value){
  if(value === null || value === undefined || String(value).trim() === ''){
    return '';
  }

  const raw = String(value).trim();

  let year;
  let month;
  let day;

  let match = raw.match(/^(\d{2})(\d{2})(\d{4})$/);

  if(match){
    day = Number(match[1]);
    month = Number(match[2]);
    year = Number(match[3]);
  }else{
    match = raw.match(/^(\d{2})[./](\d{2})[./](\d{4})$/);
  }

  if(match){
    day = Number(match[1]);
    month = Number(match[2]);
    year = Number(match[3]);
  }else{
    match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);

    if(!match){
      throw new Error(
        'passport_date должен иметь формат ДДММГГГГ, ДД/ММ/ГГГГ, ДД.ММ.ГГГГ или ГГГГ-ММ-ДД'
      );
    }

    year = Number(match[1]);
    month = Number(match[2]);
    day = Number(match[3]);
  }

  if(!isRealDate(year, month, day)){
    throw new Error('passport_date содержит некорректную дату');
  }

  return (
    `${String(year).padStart(4, '0')}-` +
    `${String(month).padStart(2, '0')}-` +
    `${String(day).padStart(2, '0')}`
  );
}

function validateQrBatchData(data){
  if(
    data === null ||
    typeof data !== 'object' ||
    Array.isArray(data)
  ){
    throw new Error('QR-сканер вернул некорректные данные');
  }

  const productCount = Number(data.product_count);

  if(
    !Number.isInteger(productCount) ||
    productCount < 1
  ){
    throw new Error(
      'product_count должен быть целым числом больше нуля'
    );
  }

  let itemsMass = null;

  if(
    data.items_mass !== null &&
    data.items_mass !== undefined &&
    String(data.items_mass).trim() !== ''
  ){
    itemsMass = Number(
      String(data.items_mass).trim().replace(',', '.')
    );

    if(!Number.isFinite(itemsMass) || itemsMass <= 0){
      throw new Error(
        'items_mass должен быть числом больше нуля'
      );
    }
  }

  return {
    passportDate: qrDateToInputDate(data.passport_date),
    productCount,
    itemsMass,
  };
}

function fillNewBatchFromQr(data, normalized){
  /*
   * Значения записываются только после полной проверки JSON.
   * Поэтому ошибочный QR не изменит уже заполненную форму.
   */
  setInputValue('nb_passport_number', data.passport_number);
  setInputValue('nb_passport_date', normalized.passportDate);

  setInputValue('nb_product_code', data.product_code);
  setInputValue('nb_product_name', data.product_name);

  setInputValue('nb_blank_alloy', data.blank_alloy);
  setInputValue('nb_blank_name', data.blank_name);

  setInputValue('nb_cert_number', data.cert_number);
  setInputValue('nb_rod_batch_number', data.rod_batch_number);

  setInputValue('nb_prod_tsi', data.prod_tsi);
  setInputValue('nb_prod_tsb', data.prod_tsb);
  setInputValue('nb_draw_rev', data.draw_rev);
  setInputValue('nb_prod_fe', data.prod_fe);
  setInputValue('nb_prod_opt1', data.prod_opt1);
  setInputValue('nb_prod_opt2', data.prod_opt2);
  setInputValue('nb_prod_opt3', data.prod_opt3);
  setInputValue('nb_prod_opt4', data.prod_opt4); 
  setInputValue('nb_prod_comment', data.prod_comment);

  setInputValue(
    'nb_items_mass',
    normalized.itemsMass === null ? '' : normalized.itemsMass
  );

  setInputValue(
    'nb_product_count',
    normalized.productCount
  );

  /*
   * cell_no не берём из QR — он определяется выбранной ячейкой.
   * Раскладка и противоволна задаются в системных настройках.
   */
}
// QR-code functions END


document.addEventListener('keydown', event => {
  if(event.key !== 'Escape') return;

  const modal = document.getElementById('rbTareModal');
  if(!modal || modal.classList.contains('hidden')) return;

  /*
   * Escape не должен прерывать обязательную процедуру замены тары.
   * Capture + stopImmediatePropagation блокируют и возможный общий
   * обработчик модалок, добавленный в другом месте страницы.
   */
  event.preventDefault();
  event.stopImmediatePropagation();
}, {capture:true});


function pad2(value){
  return String(value).padStart(2, '0');
}


function fmtDur(ms){
  const totalSec = Math.max(
    0,
    Math.floor(Number(ms || 0) / 1000)
  );

  const hours = Math.floor(totalSec / 3600);
  const minutes = Math.floor(
    (totalSec % 3600) / 60
  );
  const seconds = totalSec % 60;

  return (
    `${pad2(hours)}:` +
    `${pad2(minutes)}:` +
    `${pad2(seconds)}`
  );
}


function parseServerTimestampMs(value){
  if(value === null || value === undefined){
    return NaN;
  }

  if(typeof value === 'number'){
    if(!Number.isFinite(value)) return NaN;
    return value < 1e12 ? value * 1000 : value;
  }

  const raw = String(value).trim();
  if(!raw) return NaN;

  const hasTimezone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw);
  return Date.parse(hasTimezone ? raw : `${raw}Z`);
}


function renderCurrentDateTime(){
  const el = document.getElementById(
    'currentDateTime'
  );

  if(!el) return;

  const now = new Date();

  el.textContent = (
    `${pad2(now.getDate())}.` +
    `${pad2(now.getMonth() + 1)}.` +
    `${now.getFullYear()} ` +
    `${pad2(now.getHours())}:` +
    `${pad2(now.getMinutes())}:` +
    `${pad2(now.getSeconds())}`
  );
}


async function loadWorkplaceUser(){
  const el = document.getElementById(
    'workplaceUser'
  );

  if(!el) return;

  try{
    const user = await api('auth/me');
    const userName = String(
      user?.display_name || ''
    ).trim();

    el.textContent = userName || '—';

    const isAdmin = String(user?.role || '') === 'admin';
    workplaceIsAdmin = isAdmin;
    el.classList.toggle('admin', isAdmin);
    if(isAdmin){
      el.href = 'ui';
      el.title = 'Открыть административный интерфейс';
    }else{
      el.removeAttribute('href');
      el.removeAttribute('title');
    }
    renderWorkplaceAdminControls();
  }catch(e){
    workplaceIsAdmin = false;
    el.textContent = 'Не авторизован';
    el.classList.remove('admin');
    el.removeAttribute('href');
    el.removeAttribute('title');
    renderWorkplaceAdminControls();
  }
}


async function logoutUser(){
  try{
    await api('auth/logout', {method:'POST'});
  }catch(e){
    // Даже при сетевой ошибке возвращаем пользователя на страницу входа.
  }
  location.replace('login');
}


function setStatusField(id, value, title=''){
  const el = document.getElementById(id);
  if(!el) return;

  el.classList.remove(
    'statusOk',
    'statusBad',
    'statusPaused',
    'statusUnknown'
  );

  if(value === true){
    el.classList.add('statusOk');
  }else if(value === false){
    el.classList.add('statusBad');
  }else{
    el.classList.add('statusUnknown');
  }

  el.title = title || '';
}


function renderAutoModeStatus(){
  const el = document.getElementById('autoModeStatus');
  if(!el) return;

  const mode = String(stateCache?.mode || '');

  el.classList.remove(
    'statusOk',
    'statusBad',
    'statusPaused',
    'statusUnknown'
  );

  if(mode === 'auto_running'){
    el.textContent = 'Работа';
    el.classList.add('statusOk');
  }else if(mode.startsWith('paused')){
    el.textContent = 'Пауза';
    el.classList.add('statusPaused');
  }else if(mode === 'idle'){
    el.textContent = 'Остановлен';
    el.classList.add('statusBad');
  }else{
    el.textContent = mode || '-';
    el.classList.add('statusUnknown');
  }

  el.title = mode || '';
}


function renderRejectBinHeader(){
  const st = stateCache;
  const fillEl = document.getElementById('reject');
  const tareEl = document.getElementById(
    'reject_tare_no'
  );

  if(tareEl){
    tareEl.textContent = (
      Number.isInteger(rejectBinTareNo) &&
      rejectBinTareNo >= 1 &&
      rejectBinTareNo <= 10
    )
      ? String(rejectBinTareNo)
      : '-';
  }

  if(!fillEl) return;

  fillEl.classList.remove(
    'rejectNormal',
    'rejectNear',
    'rejectFull',
    'rejectUnknown'
  );

  if(!st){
    fillEl.textContent = '-';
    fillEl.classList.add('rejectUnknown');
    return;
  }

  const count = Math.max(
    0,
    Number(st.reject_bin_count || 0)
  );

  const capacity = Math.max(
    0,
    Number(st.reject_bin_capacity || 0)
  );

  fillEl.textContent = capacity > 0
    ? `${count}/${capacity}`
    : `${count}/-`;

  if(capacity <= 0){
    fillEl.classList.add('rejectUnknown');
    return;
  }

  if(count >= capacity){
    fillEl.classList.add('rejectFull');
    return;
  }

  const nearFullValue = Math.max(
    0,
    Math.floor(Number(rejectBinNearFull || 0))
  );

  const nearFullThreshold = Math.max(
    0,
    capacity - nearFullValue
  );

  if(
    nearFullValue > 0 &&
    count >= nearFullThreshold
  ){
    fillEl.classList.add('rejectNear');
  }else{
    fillEl.classList.add('rejectNormal');
  }
}


function renderEquipmentHeader(){
  const st = stateCache;

  setStatusField(
    'safety',
    st ? !!st.safety_ok : null,
    st
      ? (
          st.safety_ok
            ? 'Аварийный контур в норме'
            : 'Аварийный контур нарушен'
        )
      : ''
  );

  setStatusField(
    'air_pressure',
    st
      ? st.air_pressure_ok !== false
      : null,
    st
      ? (
          st.air_pressure_ok !== false
            ? 'Давление воздуха в норме'
            : 'Нет давления воздуха'
        )
      : ''
  );

  const rejectBinReplacementActive = (
    String(st?.mode || '') === 'paused_rejectbin'
  );

  setStatusField(
    'trash',
    st ? !!st.trash_present : null,
    st
      ? (
          rejectBinReplacementActive
            ? (
                st.trash_present
                  ? 'Тара находится на датчике'
                  : 'Тара снята с датчика'
              )
            : (
                st.trash_present
                  ? 'Тара брака установлена'
                  : 'Тара брака отсутствует'
              )
        )
      : ''
  );

  setStatusField(
    'rtk',
    st ? !!st.rtk_connected : null,
    st
      ? (
          `${st.rtk_connected ? 'РТК онлайн' : 'РТК офлайн'}; ` +
          `R1: ${st.rtk_action_r1 || '-'}; ` +
          `R2: ${st.rtk_action_r2 || '-'}`
        )
      : ''
  );

  setStatusField(
    'robotR1',
    rtkConnectedR1,
    rtkConnectedR1 === true
      ? 'RS013N подключён'
      : rtkConnectedR1 === false
      ? 'RS013N отключён'
      : ''
  );

  setStatusField(
    'robotR2',
    rtkConnectedR2,
    rtkConnectedR2 === true
      ? 'RS007L подключён'
      : rtkConnectedR2 === false
      ? 'RS007L отключён'
      : ''
  );

  setStatusField(
    'imConnection',
    imConnected,
    imConnected === true
      ? 'Микрометр подключён'
      : imConnected === false
      ? 'Микрометр отключён'
      : ''
  );

  const imProgramEl = document.getElementById(
    'imLoadedProgram'
  );

  if(imProgramEl){
    imProgramEl.textContent = (
      String(imLoadedProgram || '').trim() || '-'
    );
  }

  renderRejectBinHeader();
}


function fmt1(v){
  if(v === null || v === undefined) return '-';
  const n = Number(v);
  if(!Number.isFinite(n)) return '-';
  return n.toFixed(1);
}


function renderTemperatureField(
  fieldId,
  valueId,
  unitId,
  value,
  status,
  title=''
){
  const field = document.getElementById(fieldId);
  const valueEl = document.getElementById(valueId);
  const unitEl = document.getElementById(unitId);

  if(!field || !valueEl || !unitEl) return;

  field.classList.remove(
    'tempOk',
    'tempNear',
    'tempBad',
    'tempUnknown'
  );

  const normalizedStatus = String(
    status || 'unknown'
  );

  if(normalizedStatus === 'ok'){
    field.classList.add('tempOk');
  }else if(normalizedStatus === 'near_critical'){
    field.classList.add('tempNear');
  }else if(
    normalizedStatus === 'out_of_range' ||
    normalizedStatus === 'missing'
  ){
    field.classList.add('tempBad');
  }else{
    field.classList.add('tempBad');
  }

  const numberValue = Number(value);
  const hasValue = (
    value !== null &&
    value !== undefined &&
    Number.isFinite(numberValue)
  );

  valueEl.textContent = hasValue
    ? numberValue.toFixed(1)
    : '-';
  unitEl.textContent = hasValue ? '°C' : '';
  field.title = title || '';
}


const OPERATOR_MESSAGE_SEVERITY_ORDER = {
  info: 0,
  warn: 1,
  error: 2,
};

const OPERATOR_MESSAGE_ICONS = {
  info: 'ℹ',
  warn: '⚠',
  error: '✖',
};

function normalizeMessageSeverity(value){
  const severity = String(value || 'info').toLowerCase();
  return ['info', 'warn', 'error'].includes(severity)
    ? severity
    : 'info';
}

function normalizeOperatorMessages(st){
  const rawMessages = Array.isArray(st?.operator_messages)
    ? st.operator_messages
    : [];

  const messages = [];
  const seenKeys = new Set();

  for(const raw of rawMessages){
    const message = String(raw?.message || '').trim();
    if(!message) continue;

    const key = String(raw?.key || '').trim();
    const severity = normalizeMessageSeverity(raw?.severity);
    const dedupeKey = key
      ? `key:${key}`
      : `message:${severity}:${message}`;

    if(seenKeys.has(dedupeKey)) continue;
    seenKeys.add(dedupeKey);

    messages.push({
      key,
      message,
      severity,
      sourceOrder: messages.length,
    });
  }

  // Совместимость со старым API: legacy-поле используется только тогда,
  // когда новый реестр отсутствует или не содержит валидных сообщений.
  if(messages.length === 0){
    const legacyMessage = String(st?.message || '').trim();

    if(legacyMessage){
      messages.push({
        key: 'legacy.message',
        message: legacyMessage,
        severity: normalizeMessageSeverity(st?.message_severity),
        sourceOrder: messages.length,
      });
    }
  }

  messages.sort((left, right) => {
    const severityDiff = (
      OPERATOR_MESSAGE_SEVERITY_ORDER[left.severity]
      - OPERATOR_MESSAGE_SEVERITY_ORDER[right.severity]
    );

    if(severityDiff !== 0){
      return severityDiff;
    }

    return left.sourceOrder - right.sourceOrder;
  });

  return messages;
}

function renderMessages(st){
  const panel = document.getElementById('msg');
  if(!panel) return;

  const messages = normalizeOperatorMessages(st);

  // Полностью заменяем содержимое при каждом state-refresh. Поэтому
  // повторные SSE-события и реконнекты не накапливают дубли в DOM.
  panel.replaceChildren();

  if(messages.length === 0){
    panel.className = 'messagesPanel empty';
    return;
  }

  panel.className = 'messagesPanel';

  for(const item of messages){
    const row = document.createElement('div');
    row.className = `operatorMessage ${item.severity}`;

    const icon = document.createElement('span');
    icon.className = 'operatorMessageIcon';
    icon.setAttribute('aria-hidden', 'true');
    icon.textContent = OPERATOR_MESSAGE_ICONS[item.severity] || 'ℹ';

    const text = document.createElement('span');
    text.className = 'operatorMessageText';
    text.textContent = item.message;

    row.append(icon, text);

    if(item.key){
      row.dataset.messageKey = item.key;
    }

    panel.appendChild(row);
  }
}

async function loadTemps(force=false){
  const now = Date.now();

  if(tempsFetchInFlight){
    if(!force){
      return tempsFetchInFlight;
    }

    try{
      await tempsFetchInFlight;
    }catch(e){}
  }

  if(!force && now - tempsFetchedAtMs < 800){
    return;
  }

  tempsFetchInFlight = (async ()=>{
    try{
      const s = await api('settings');
      const tareNo = Number(
        s.reject_bin_tare_no
      );

      rejectBinTareNo = (
        Number.isInteger(tareNo) &&
        tareNo >= 1 &&
        tareNo <= 10
      )
        ? tareNo
        : null;

      rejectBinNearFull = Math.max(
        0,
        Math.floor(Number(s.rjb_near_full || 0))
      );

      stopAfterCurrentBatch = !!(
        s.stop_after_current_batch
      );

      const requestedStopBatchId = Number(
        s.stop_after_batch_id
      );
      stopAfterBatchId = (
        Number.isInteger(requestedStopBatchId) &&
        requestedStopBatchId > 0
      )
        ? requestedStopBatchId
        : null;

      rtkConnectedR1 = (
        typeof s.rtk_connected_r1 === 'boolean'
          ? s.rtk_connected_r1
          : null
      );

      rtkConnectedR2 = (
        typeof s.rtk_connected_r2 === 'boolean'
          ? s.rtk_connected_r2
          : null
      );

      imConnected = (
        typeof s.im_connected === 'boolean'
          ? s.im_connected
          : null
      );

      imLoadedProgram = String(
        s.im_loaded_program || ''
      ).trim();

      renderTemperatureField(
        'temp_loading_field',
        'temp_loading',
        'temp_loading_unit',
        s.temperature_sensor_loading_value,
        s.temperature_sensor_loading_status,
        (
          `Постамат: ${fmt1(s.temperature_sensor_loading_value)} °C; ` +
          `допуск ${fmt1(s.temperature_sensor_loading_min)}..` +
          `${fmt1(s.temperature_sensor_loading_max)} °C`
        )
      );

      renderTemperatureField(
        'temp_im_field',
        'temp_im',
        'temp_im_unit',
        s.temperature_sensor_im_value,
        s.temperature_sensor_im_status,
        (
          `Измерительная машина: ${fmt1(s.temperature_sensor_im_value)} °C; ` +
          `допуск ${fmt1(s.temperature_sensor_im_min)}..` +
          `${fmt1(s.temperature_sensor_im_max)} °C`
        )
      );

      emergencyActive = !!s.emergency_active;
      emergencySoundMuted = !!s.emergency_sound_muted;

      renderEquipmentHeader();
      renderEmergencySoundButton();
      renderStopAfterCurrentBatch();

    }catch(e){
      // Молча оставляем прежнее состояние интерфейса.
    }finally{
      tempsFetchedAtMs = Date.now();
      tempsFetchInFlight = null;
    }
  })();

  return tempsFetchInFlight;
}

function openNewBatchModal(cellNo){
  newBatchCellNo = cellNo;
  const titleEl = document.getElementById('nb_title');
  if(titleEl){
    titleEl.textContent = `Новая партия. Ячейка ${cellNo}`;
  }

  stopQrScan('', 'empty');

  document.getElementById('nbModal').classList.remove('hidden');
}

function closeNewBatchModal(){
  stopQrScan('', 'empty');

  document.getElementById('nbModal').classList.add('hidden');
  newBatchCellNo = null;
}

function nbBackdropClick(e){
  if(e.target && e.target.id === 'nbModal') closeNewBatchModal();
}


function clearNewBatch(){
  [
    'nb_passport_number','nb_passport_date','nb_product_code','nb_product_name',
    'nb_blank_alloy','nb_blank_name','nb_cert_number','nb_rod_batch_number',
    'nb_items_mass','nb_product_count',
    'nb_prod_tsi','nb_prod_tsb','nb_draw_rev','nb_prod_fe',
    'nb_prod_opt1','nb_prod_opt2','nb_prod_opt3','nb_prod_opt4',
    'nb_prod_comment'
  ].forEach(id => {
    const el = document.getElementById(id);
    if(el) el.value = '';
  });

  stopQrScan('', 'empty');
}


function setExtractStatus(message, kind='empty'){
  const el = document.getElementById('ex_status');
  if(!el) return;

  const allowed = ['empty', 'waiting', 'ok', 'error'];
  const cls = allowed.includes(kind) ? kind : 'empty';

  el.className = `qrStatus ${cls}`;
  el.textContent = message || '';
}

function setExtractionBusy(value){
  extractionBusy = !!value;

  document
    .querySelectorAll('#exModal button')
    .forEach(button => {
      /*
       * Закрытие модалки всегда доступно. Это важно при
       * неисправности замка, датчика или потере связи.
       */
      if(button.id === 'exCloseBtn'){
        button.disabled = false;
        return;
      }

      button.disabled = extractionBusy;
    });
}

function resetExtractModal(){
  extractionStage = 'idle';
  extractionDocsPrinted = false;
  extractionBusy = false;

  const title = document.getElementById('stepTitle');
  const info = document.getElementById('stepInfo');
  const docsBox = document.getElementById('docs');
  const actions = document.getElementById('stepActions');

  if(title) title.textContent = '';
  if(info) info.textContent = '';
  if(docsBox) docsBox.innerHTML = '';
  if(actions) actions.innerHTML = '';

  setExtractStatus('', 'empty');
}

function openExtractModal(batch){
  resetExtractModal();

  const data = (batch && batch.data) ? batch.data : {};
  const passNo = data.passport_number || data.product_code || '-';

  document.getElementById('exTitle').textContent =
    `Извлечение партии #${batch.id} · ${passNo}`;

  document.getElementById('exModal').classList.remove('hidden');
}

function closeExtractModal(){
  /*
   * Закрываем только окно. Незавершённое состояние процедуры
   * сохраняем, чтобы оператор мог пользоваться остальным
   * интерфейсом и затем продолжить извлечение той же партии.
   */
  document.getElementById('exModal').classList.add('hidden');
  return true;
}

function exBackdropClick(e){
  if(e.target && e.target.id === 'exModal'){
    closeExtractModal();
  }
}

function finishAndCloseExtractModal(){
  currentBatch = null;
  document.getElementById('exModal').classList.add('hidden');
  resetExtractModal();
}


function inputDateToPassportDate(value){
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);

  if(!match){
    throw new Error(
      'Дата маршрутного паспорта должна быть введена в формате дд/мм/гггг.'
    );
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);

  if(!isRealDate(year, month, day)){
    throw new Error('Дата маршрутного паспорта содержит некорректное значение.');
  }

  return (
    `${String(day).padStart(2, '0')}/` +
    `${String(month).padStart(2, '0')}/` +
    `${String(year).padStart(4, '0')}`
  );
}


async function submitNewBatch(){
  if(!newBatchCellNo){
    setQrStatus(
      'Не выбрана загрузочная ячейка. Закройте окно и выберите «+ партия».',
      'error'
    );
    return;
  }

  setQrStatus('', 'empty');

  const requiredFields = [
    ['nb_passport_number', 'Маршрутный паспорт'],
    ['nb_product_name', 'Наименование детали'],
    ['nb_passport_date', 'Дата маршрутного паспорта'],
    ['nb_product_code', 'Обозначение детали'],
    ['nb_prod_fe', 'Содержание железа'],
    ['nb_blank_alloy', 'Сплав'],
    ['nb_draw_rev', 'Номер изменения чертежа'],
    ['nb_blank_name', 'Заготовка'],
    ['nb_cert_number', 'Номер сертификата'],
    ['nb_prod_tsi', 'ТУ на слиток'],
    ['nb_rod_batch_number', 'Номер партии прутка'],
    ['nb_prod_tsb', 'ТУ на заготовку'],
  ];

  for(const [id, label] of requiredFields){
    if(v(id) !== '') continue;

    setQrStatus(
      `Поле «${label}» обязательно для заполнения.`,
      'error'
    );
    document.getElementById(id)?.focus();
    return;
  }

  let passport_date;

  try{
    passport_date = inputDateToPassportDate(
      v('nb_passport_date')
    );
  }catch(e){
    setQrStatus(
      e?.message || 'Дата маршрутного паспорта введена некорректно.',
      'error'
    );
    document.getElementById('nb_passport_date')?.focus();
    return;
  }

  const product_count = Number(v('nb_product_count') || 0);

  if(!Number.isInteger(product_count) || product_count < 1){
    setQrStatus(
      'Количество должно быть целым числом больше нуля.',
      'error'
    );
    document.getElementById('nb_product_count')?.focus();
    return;
  }

  const items_mass = Number(v('nb_items_mass'));

  if(!Number.isFinite(items_mass) || items_mass <= 0){
    setQrStatus(
      'Масса должна быть числом больше нуля.',
      'error'
    );
    document.getElementById('nb_items_mass')?.focus();
    return;
  }

  const body = {
    cell_no: newBatchCellNo,
    passport_number: v('nb_passport_number'),
    passport_date: passport_date,
    product_code: v('nb_product_code'),
    product_name: v('nb_product_name'),
    blank_alloy: v('nb_blank_alloy'),
    blank_name: v('nb_blank_name'),
    cert_number: v('nb_cert_number'),
    rod_batch_number: v('nb_rod_batch_number'),
    items_mass: items_mass,
    prod_tsi: v('nb_prod_tsi'),
    prod_tsb: v('nb_prod_tsb'),
    draw_rev: v('nb_draw_rev'),
    prod_fe: v('nb_prod_fe'),
    prod_opt1: v('nb_prod_opt1') || null,
    prod_opt2: v('nb_prod_opt2') || null,
    prod_opt3: v('nb_prod_opt3') || null,
    prod_opt4: v('nb_prod_opt4') || null,
    prod_comment: v('nb_prod_comment') || null,
    product_count: product_count,
  };

  try{
    await api('batches', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });

    clearNewBatch();
    closeNewBatchModal();
    await loadBatches();

  }catch(e){
    const message = apiErrorMessage(
      e,
      'Не удалось создать партию.'
    );

    setQrStatus(message, 'error');
  }
}


function startExtractByBatchId(id){
  const b = (batchesCache||[]).find(x => x.id === id);
  if(!b){ alert('Партия не найдена'); return; }
  startExtractByBatch(b);
}

function nowIso(){ return new Date().toISOString(); }

async function api(url, opts){
  const r = await fetch(url, opts);
  if(r.status === 401){
    location.replace('login');
    throw {detail:'Требуется авторизация.'};
  }
  const t = await r.text();
  let j; try{ j = JSON.parse(t);}catch(e){ j=t; }
  if(!r.ok){ throw j; }
  return j;
}

async function sendCommand(type, payload){
  return api('commands', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({type, payload: payload || {}})
  });
}


function sleepMs(ms){
  return new Promise(resolve => setTimeout(resolve, ms));
}


async function waitCommandDone(commandId, timeoutMs=15000){
  const deadline = Date.now() + timeoutMs;

  while(Date.now() < deadline){
    const cmd = await api(`commands/${Number(commandId)}`);
    const status = String(cmd?.status || '').toLowerCase();

    const waitingStatuses = [
      'pending',
      'processing',
      'running',
      'claimed',
      'in_progress'
    ];

    if(waitingStatuses.includes(status)){
      await sleepMs(200);
      continue;
    }

    if(
      cmd?.error ||
      status === 'failed' ||
      status === 'error'
    ){
      throw {
        detail: (
          cmd?.error ||
          'Команда завершилась с ошибкой'
        )
      };
    }

    return cmd;
  }

  throw {
    detail: 'Не получен результат выполнения команды'
  };
}


async function sendCommandAndWait(
  type,
  payload,
  timeoutMs=15000
){
  const cmd = await sendCommand(type, payload);

  if(!cmd?.id){
    throw {
      detail: 'Сервер не вернул идентификатор команды'
    };
  }

  return waitCommandDone(cmd.id, timeoutMs);
}


function setRejectBinTareStatus(message, kind='empty'){
  const el = document.getElementById('rb_tare_status');
  if(!el) return;

  const allowed = ['empty', 'waiting', 'ok', 'error'];
  const cls = allowed.includes(kind) ? kind : 'empty';

  el.className = `qrStatus ${cls}`;
  el.textContent = message || '';
}


function openRejectBinTareModal(options={}){
  rejectBinModalManual = options.manual === true;

  const modal = document.getElementById('rbTareModal');
  const input = document.getElementById('rb_tare_no');

  if(!modal) return;

  setRejectBinTareStatus('', 'empty');
  modal.classList.remove('hidden');

  if(input){
    input.value = '';
    setTimeout(()=>{
      try{
        input.focus();
        input.select();
      }catch(e){}
    }, 0);
  }
}


function closeRejectBinTareModal(){
  /*
   * Модалка замены тары является обязательным шагом процедуры.
   * Пользователь не должен закрывать её вручную ни кнопкой,
   * ни кликом по фону, ни из старого обработчика разметки.
   *
   * Окно закрывается только программно после успешного принятия
   * номера либо когда syncRejectBinTareModal() получает от сервера,
   * что ввод номера больше не требуется.
   */
  return false;
}


function rbTareBackdropClick(event){
  /*
   * Защитный no-op для старой закэшированной разметки, где у overlay
   * ещё мог сохраниться onclick="rbTareBackdropClick(event)".
   */
  if(event){
    event.preventDefault();
    event.stopPropagation();
  }
  return false;
}


async function submitRejectBinTare(){
  if(rejectBinSubmitInFlight) return;

  const raw = String(
    document.getElementById('rb_tare_no')?.value || ''
  ).trim();

  if(!/^(?:[1-9]|10)$/.test(raw)){
    setRejectBinTareStatus(
      'Введите целый номер тары от 1 до 10.',
      'error'
    );
    return;
  }

  const tareNo = Number(raw);
  const button = document.getElementById('rb_tare_submit');

  rejectBinSubmitInFlight = true;

  if(button){
    button.disabled = true;
  }

  setRejectBinTareStatus(
    `Сохраняется номер тары №${tareNo}…`,
    'waiting'
  );

  try{
    await sendCommandAndWait(
      'REPLACE_REJECTBIN',
      {tare_no: tareNo}
    );

    setRejectBinTareStatus(
      `Номер тары №${tareNo} принят.`,
      'ok'
    );

    rejectBinModalManual = false;

    await loadState();

    document
      .getElementById('rbTareModal')
      ?.classList.add('hidden');

  }catch(e){
    setRejectBinTareStatus(
      apiErrorMessage(
        e,
        'Не удалось принять номер тары.'
      ),
      'error'
    );
  }finally{
    rejectBinSubmitInFlight = false;

    if(button){
      button.disabled = false;
    }
  }
}


async function syncRejectBinTareModal(st){
  const pausedRejectBin = (
    String(st?.mode || '') === 'paused_rejectbin'
  );

  /*
   * Ручную модалку оператор открыл сам до постановки команды.
   * Обычное обновление state не должно её закрывать.
   */
  if(!pausedRejectBin){
    if(!rejectBinModalManual && !rejectBinSubmitInFlight){
      document
        .getElementById('rbTareModal')
        ?.classList.add('hidden');
    }
    return;
  }

  if(rejectBinStatusInFlight) return;

  rejectBinStatusInFlight = true;

  try{
    const status = await api(
      'reject-bin/replacement-status'
    );

    if(status?.tare_no_required){
      const modal = document.getElementById('rbTareModal');

      if(modal?.classList.contains('hidden')){
        openRejectBinTareModal({manual:false});
      }
    }else if(!rejectBinSubmitInFlight){
      rejectBinModalManual = false;

      document
        .getElementById('rbTareModal')
        ?.classList.add('hidden');
    }
  }catch(e){
    /*
     * Ошибка служебной проверки не должна ломать
     * основное обновление интерфейса.
     */
  }finally{
    rejectBinStatusInFlight = false;
  }
}


function isCycleOn(){
  const st = stateCache || {};
  return !!st.rtk_connected && !st.need_cycle_on;
}

async function startAuto(){
  if(!isCycleOn()) return;
  if(isPausedSystemMode()) return;
  if(stateCache?.air_pressure_ok === false) return;

  if(
    String(stateCache?.mode || '') ===
    'auto_running'
  ){
    return;
  }

  try{
    await sendCommand('START_AUTO', {});
  }catch(e){
    alert(JSON.stringify(e));
  }
}

async function stopAuto(){
  if(!isCycleOn()) return;
  if(isPausedSystemMode()) return;
  try{
    await sendCommand('STOP_AUTO', {});
  }catch(e){
    alert(JSON.stringify(e));
  }
}

async function pauseSys(){
  if(!isCycleOn()) return;
  try{
    await sendCommand('PAUSE_SYSTEM', {});
  }catch(e){
    alert(JSON.stringify(e));
  }
}

async function resumeSys(){ try{ await sendCommand('RESUME_SYSTEM', {}); } catch(e){ alert(JSON.stringify(e)); } }

function replaceRB(){
  openRejectBinTareModal({manual:true});
}

async function toggleStopAfterCurrentBatch(){
  const checkbox = document.getElementById(
    'stopAfterCurrentBatchCheckbox'
  );

  if(!checkbox || stopAfterBatchToggleInFlight){
    return;
  }

  const requested = !!checkbox.checked;
  const previousEnabled = !!stopAfterCurrentBatch;
  const previousBatchId = stopAfterBatchId;
  const activeBatchId = Number(
    stateCache?.active_batch_id || 0
  );

  if(
    requested &&
    (!Number.isInteger(activeBatchId) || activeBatchId <= 0)
  ){
    checkbox.checked = previousEnabled;
    renderStopAfterCurrentBatch();
    return;
  }

  stopAfterBatchToggleInFlight = true;
  renderStopAfterCurrentBatch();

  try{
    const result = await api('settings', {
      method:'PUT',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({
        stop_after_current_batch: requested
      })
    });

    stopAfterCurrentBatch = !!(
      result?.patch?.stop_after_current_batch
    );

    const resultBatchId = Number(
      result?.patch?.stop_after_batch_id
    );
    stopAfterBatchId = (
      Number.isInteger(resultBatchId) &&
      resultBatchId > 0
    )
      ? resultBatchId
      : null;

  }catch(e){
    stopAfterCurrentBatch = previousEnabled;
    stopAfterBatchId = previousBatchId;

    alert(
      apiErrorMessage(
        e,
        'Не удалось изменить останов после партии'
      )
    );
  }finally{
    stopAfterBatchToggleInFlight = false;
    renderStopAfterCurrentBatch();
  }
}

async function cycleOn(){
  try{
    await sendCommand('RTK_CYCLE_ON', {});
    await loadState();

    // команда исполняется daemon-loop'ом асинхронно, поэтому даём ему один тик
    setTimeout(()=>{ try{ loadState(); }catch(e){} }, 500);
  }catch(e){
    alert(typeof e === 'string' ? e : JSON.stringify(e));
  }
}


let rtkModeChangeInFlight = false;
let rtkNextStepInFlight = false;

function renderEmergencySoundButton(){
  const btn = document.getElementById(
    'emergencySoundBtn'
  );

  if(!btn){
    return;
  }

  /*
   * Кнопка доступна только во время фактического emergency.
   * Пока команда выполняется, повторное нажатие запрещено.
   */
  btn.disabled = (
    !workplaceIsAdmin ||
    !emergencyActive ||
    emergencySoundToggleInFlight
  );

  btn.classList.toggle(
    'soundMuted',
    emergencySoundMuted
  );

  btn.setAttribute(
    'aria-pressed',
    emergencySoundMuted ? 'true' : 'false'
  );

  btn.textContent = emergencySoundMuted
    ? '🔇'
    : '🔊';

  const soundActionText = emergencySoundMuted
    ? 'Включить аварийный звук'
    : 'Отключить аварийный звук';

  btn.setAttribute(
    'aria-label',
    soundActionText
  );

  btn.title = !emergencyActive
    ? 'Аварийный звук сейчас не активен'
    : soundActionText;
}


function renderWorkplaceAdminControls(){
  const soundSeparator = document.getElementById(
    'emergencySoundSeparator'
  );
  const soundBtn = document.getElementById(
    'emergencySoundBtn'
  );
  const resetSeparator = document.getElementById(
    'rtkResetSeparator'
  );
  const resetBtn = document.getElementById(
    'rtkResetBtn'
  );

  [soundSeparator, soundBtn, resetSeparator, resetBtn]
    .forEach(el => {
      if(el) el.classList.toggle('hidden', !workplaceIsAdmin);
    });

  if(resetBtn){
    const st = stateCache || {};
    const cycleOn = !!st.rtk_connected && !st.need_cycle_on;

    resetBtn.disabled = (
      !workplaceIsAdmin ||
      cycleOn ||
      rtkResetInFlight
    );

    resetBtn.title = !workplaceIsAdmin
      ? ''
      : cycleOn
      ? 'СБРОС РТК доступен только при cycle_on=false'
      : rtkResetInFlight
      ? 'Команда СБРОС РТК выполняется'
      : 'Отправить чистую команду Reset на РТК';
  }

  renderEmergencySoundButton();
}


async function toggleEmergencySound(){
  if(
    !workplaceIsAdmin ||
    !emergencyActive ||
    emergencySoundToggleInFlight
  ){
    return;
  }

  const previousMuted = emergencySoundMuted;
  const nextMuted = !previousMuted;

  emergencySoundToggleInFlight = true;
  emergencySoundMuted = nextMuted;

  renderEmergencySoundButton();

  try{
    await sendCommandAndWait(
      'SET_EMERGENCY_SOUND_MUTED',
      {
        muted: nextMuted
      }
    );

    /*
     * Забираем подтверждённое состояние из settings.
     */
    await loadTemps(true);

  }catch(e){
    emergencySoundMuted = previousMuted;

    renderEmergencySoundButton();

    alert(
      apiErrorMessage(
        e,
        'Не удалось изменить аварийный звук'
      )
    );

  }finally{
    emergencySoundToggleInFlight = false;

    renderEmergencySoundButton();
  }
}


async function resetRtk(){
  if(!workplaceIsAdmin || rtkResetInFlight){
    return;
  }

  const st = stateCache || {};
  const cycleOn = !!st.rtk_connected && !st.need_cycle_on;

  if(cycleOn){
    renderWorkplaceAdminControls();
    return;
  }

  rtkResetInFlight = true;
  renderWorkplaceAdminControls();

  try{
    await sendCommandAndWait('RTK_RESET', {});
    await loadState();
  }catch(e){
    alert(
      apiErrorMessage(
        e,
        'Не удалось выполнить СБРОС РТК'
      )
    );
  }finally{
    rtkResetInFlight = false;
    renderWorkplaceAdminControls();
  }
}


async function changeRtkMode(){
  const select = document.getElementById('rtkModeSelect');
  if(!select || rtkModeChangeInFlight) return;

  const manual = select.value === 'manual';
  const previousManual = !!stateCache?.rtk_manual_mode;

  rtkModeChangeInFlight = true;
  select.disabled = true;
  renderRtkManualControls();

  try{
    await sendCommand(
      'RTK_SET_STEP_MODE',
      {manual: manual}
    );

    /*
     * Команда выполняется daemon-loop'ом асинхронно.
     * Сразу показываем выбранное значение локально,
     * затем синхронизируемся с серверным state.
     */
    if(stateCache){
      stateCache.rtk_manual_mode = manual;
    }

    renderRtkManualControls();

    setTimeout(()=>{
      try{
        loadState();
      }catch(e){}
    }, 500);

  }catch(e){
    if(stateCache){
      stateCache.rtk_manual_mode = previousManual;
    }

    select.value = previousManual ? 'manual' : 'auto';

    alert(
      apiErrorMessage(
        e,
        'Не удалось переключить режим РТК'
      )
    );
  }finally{
    rtkModeChangeInFlight = false;
    select.disabled = false;
    renderRtkManualControls();
  }
}

async function rtkNextStep(){
  const st = stateCache || {};

  if(!st.rtk_manual_mode || rtkNextStepInFlight){
    return;
  }

  rtkNextStepInFlight = true;
  renderRtkManualControls();

  try{
    await sendCommand('RTK_NEXT_STEP', {});

    setTimeout(()=>{
      try{
        loadState();
      }catch(e){}
    }, 300);

  }catch(e){
    alert(
      apiErrorMessage(
        e,
        'Не удалось выполнить следующий шаг РТК'
      )
    );
  }finally{
    rtkNextStepInFlight = false;
    renderRtkManualControls();
  }
}

function renderRtkManualControls(){
  const st = stateCache || {};

  const manual = !!st.rtk_manual_mode;
  const connected = !!st.rtk_connected;
  const cycleRunning = st.mode === 'auto_running';

  const select = document.getElementById('rtkModeSelect');
  const nextBtn = document.getElementById('rtkNextStepBtn');

  if(select && !rtkModeChangeInFlight){
    select.value = manual ? 'manual' : 'auto';
    select.disabled = !connected;
  }

  if(nextBtn){
    /*
     * Основное требование: кнопка неактивна,
     * если ручной режим не выбран.
     *
     * Дополнительно блокируем её без связи и до запуска цикла.
     */
    nextBtn.disabled = (
      !manual ||
      !connected ||
      !cycleRunning ||
      rtkModeChangeInFlight ||
      rtkNextStepInFlight
    );

    nextBtn.title = !manual
      ? 'Выберите ручной режим РТК'
      : !connected
      ? 'РТК не подключён'
      : !cycleRunning
      ? 'Сначала нажмите СТАРТ'
      : 'Выполнить следующий шаг программы РТК';
  }
}


function renderCycleOnButton(){
  const st = stateCache || {};
  const btn = document.getElementById('cycleOnBtn');
  if(!btn) return;

  /*
   * need_cycle_on=true означает, что cycle_on ещё требуется,
   * то есть текущий cycle_on=false.
   * При отсутствии связи также считаем cycle_on=false.
   */
  const cycleOn = !!st.rtk_connected && !st.need_cycle_on;

  btn.classList.toggle('cycleOnTrue', cycleOn);
  btn.classList.toggle('cycleOnFalse', !cycleOn);

  btn.title = cycleOn
    ? 'РТК включён'
    : 'РТК выключен — нажмите для отправки cycle_on';

  const airPressureOk = (
    st.air_pressure_ok !== false
  );

  const mode = String(st.mode || '');

  const autoRunning = (
    mode === 'auto_running'
  );
  const paused = mode.startsWith('paused');
  
  const startBtn = document.getElementById(
    'startAutoBtn'
  );

  if(startBtn){
    startBtn.disabled = (
      !cycleOn ||
      !airPressureOk ||
      autoRunning ||
      paused
    );

    startBtn.title = autoRunning
      ? 'Автоматический цикл уже запущен'
      : paused
      ? 'СТАРТ недоступен, пока система находится в паузе'
      : !airPressureOk
      ? 'Нет давления воздуха'
      : cycleOn
      ? ''
      : 'РТК выключен';
  }
  
  const stopBtn = document.getElementById('stopAutoBtn');
  if(stopBtn){
    const paused = isPausedSystemMode();

    stopBtn.disabled = (
      !cycleOn ||
      paused
    );

    stopBtn.title = paused
      ? 'СТОП недоступен, пока система находится в паузе'
      : cycleOn
      ? ''
      : 'РТК выключен';
    }
  
  const pauseBtn = document.getElementById('pauseSysBtn');
  if(pauseBtn){
    pauseBtn.disabled = !cycleOn;
  }
}

async function refreshAll(){
  await loadState();
  await loadTemps();
  await loadBatches();
  renderCells();
}

async function loadState(){
  const st = await api('state');
  stateCache = st;

  renderCycleOnButton();
  renderRtkManualControls();
  renderStopAfterCurrentBatch();
  renderWorkplaceAdminControls();

  const modeEl = document.getElementById('mode');

  if(modeEl){
    modeEl.textContent = st.mode || '-';
  }

  renderEquipmentHeader();
  renderAutoModeStatus();
  renderMessages(st);

  renderActiveProgress();
  renderLastMeas();

  await syncRejectBinTareModal(st);
  await loadTemps();
}

async function loadBatches(){
  const list = await api('batches?limit=200');
  batchesCache = list;
  renderActiveProgress();
  renderCells();
}

function normalizeCellIds(raw){
  if(raw === null || raw === undefined) return [];

  const xs = Array.isArray(raw) ? raw : [raw];

  return [...new Set(
    xs
      .map(x => Number(x))
      .filter(x => Number.isInteger(x) && x > 0)
  )];
}

function batchCellIds(batch, kind){
  const data = batch?.data || {};
  const loc = batch?.location || {};

  const productCode = String(data.product_code || '').trim();

  if(kind === 'loading'){
    const ids = normalizeCellIds(
      data.in_tare_ids ?? loc.in_tare_ids
    );

    if(ids.length > 0) return ids;

    // cell_no исторически обозначает только загрузочную ячейку.
    return normalizeCellIds(
      loc.cell_no ?? data.cell_no
    );
  }

  if(kind === 'unloading'){
    // Специальная деталь всегда отображается только в ячейке выгрузки №16.
    if(SPECIAL_UNLOADING_PRODUCT_CODES.has(productCode)){
      return [Number(SPECIAL_UNLOADING_CELL)];
    }

    const ids = normalizeCellIds(
      data.out_tare_ids ?? loc.out_tare_ids
    );

    if(ids.length > 0) return ids;

    // Fallback только для старых обычных партий.
    return normalizeCellIds(
      loc.cell_no ?? data.cell_no
    );
  }

  return [];
}

function findBatchByCell(cellNo, kind){
  const xs = (batchesCache || []).filter(b => {
    if(b.status === 'extracted') return false;
    return batchCellIds(b, kind).includes(Number(cellNo));
  });

  if(xs.length === 0) return null;

  const pr = (s) => (
    s === 'auto_processing' ? 0 :
    s === 'loaded' ? 1 :
    s === 'blocked' ? 2 :
    s === 'done' ? 3 :
    s === 'rejected' ? 4 : 9
  );

  xs.sort((a, b) => pr(a.status) - pr(b.status));
  return xs[0];
}

function cellClass(b){
  if(!b) return 'cell';

  if(b.status === 'auto_processing'){
    return 'cell auto';
  }

  if(b.status === 'loaded'){
    return 'cell loaded';
  }

  if(b.status === 'rejected'){
    return 'cell rejected';
  }

  if(b.status === 'done'){
    return 'cell done';
  }

  if(b.status === 'blocked'){
    return 'cell blocked';
  }

  return 'cell busy';
}


function cellStatusLabel(status){
  const labels = {
    auto_processing: 'авто',
    loaded: 'загр.',
    rejected: 'откл.',
    done: 'готово',
    blocked: 'блок.',
  };

  return labels[String(status || '')] || String(status || '');
}


function canExtractBatch(b){
  if(!b) return false;

  const statusOk = ['done', 'rejected'].includes(
    String(b.status || '')
  );

  const extractionReady = (
    b?.data?.extraction_ready !== false
  );

  return statusOk && extractionReady;
}


async function openLoading(idx){
  try{ await sendCommand('POSTAMAT_OPEN_LOADING_CELL', {idx}); } catch(e){ alert(JSON.stringify(e)); }
}

async function openUnloading(idx){
  try{ await sendCommand('POSTAMAT_OPEN_UNLOADING_CELL', {idx}); } catch(e){ alert(JSON.stringify(e)); }
}

async function openCellsForBatch(batchId){
  const result = await api(
    `batches/${batchId}/open_cells`,
    {method:'POST'}
  );

  const commandIds = Array.isArray(result?.command_ids)
    ? result.command_ids
    : [];

  for(const commandId of commandIds){
    await waitCommandDone(Number(commandId), 20000);
  }

  return result;
}

async function activateBatch(batchId){
  try{ await api(`batches/${batchId}/activate`, {method:'POST'}); }
  catch(e){ alert(JSON.stringify(e)); }
}


function canPrintBatchDocs(b){
  return !!b && ['done', 'rejected', 'extracted'].includes(String(b.status || ''));
}

function isActiveAutoBatch(b){
  if(!b) return false;

  return (
    String(b.status || '') === 'auto_processing' &&
    Number(stateCache?.active_batch_id || 0) === Number(b.id)
  );
}

async function abortActiveBatchForExtraction(b){
  try{
    await sendCommand(
      'ABORT_ACTIVE_BATCH_FOR_EXTRACTION',
      {batch_id: Number(b.id)}
    );

    await loadState();
    await loadBatches();

    return (batchesCache || []).find(x => Number(x.id) === Number(b.id)) || {
      ...b,
      status: 'rejected'
    };

  }catch(e){
    alert(
      apiErrorMessage(
        e,
        'Не удалось аварийно остановить активную партию для извлечения'
      )
    );
    return null;
  }
}


async function startExtractByBatch(b){
  /*
   * Если процедура этой партии уже начата и модалка была
   * закрыта, просто показываем её снова без повторного запуска
   * команд и без потери текущего шага.
   */
  if(
    currentBatch &&
    Number(currentBatch.id) === Number(b?.id) &&
    extractionStage !== 'idle' &&
    extractionStage !== 'complete'
  ){
    document
      .getElementById('exModal')
      ?.classList.remove('hidden');
    return;
  }

  if(
    currentBatch &&
    Number(currentBatch.id) !== Number(b?.id) &&
    extractionStage !== 'idle' &&
    extractionStage !== 'complete'
  ){
    alert(
      `Сначала завершите или продолжите извлечение партии #${currentBatch.id}.`
    );
    return;
  }

  if(!canExtractBatch(b)){
    const status = String(b?.status || '-');

    const extractionPending = (
      b?.data?.extraction_ready === false
    );

    const hint = status === 'auto_processing'
      ? ' Сначала нажмите СТОП и дождитесь безопасного завершения роботов.'
      : extractionPending
      ? ' Роботы ещё завершают безопасную остановку.'
      : '';

    alert(
      `Извлечение партии сейчас недоступно. ` +
      `Текущий статус: ${status}.${hint}`
    );
    return;
  }

  currentBatch = b;
  openExtractModal(b);
  extractionStage = 'opening';

  const loadingCells = batchCellIds(b, 'loading');
  const unloadingCells = batchCellIds(b, 'unloading');

  const loadingText = loadingCells.length
    ? loadingCells.join(', ')
    : '-';
  const unloadingText = unloadingCells.length
    ? unloadingCells.join(', ')
    : '-';

  document.getElementById('stepTitle').textContent =
    `Загрузка: №${loadingText}; выгрузка: №${unloadingText}`;

  document.getElementById('stepInfo').textContent =
    'Шаг 1 из 3: открываются ячейки партии.';

  document.getElementById('docs').innerHTML = '';
  document.getElementById('stepActions').innerHTML = '';

  setExtractStatus(
    'Команды открытия отправлены. Ожидание подтверждения постаматов…',
    'waiting'
  );

  setExtractionBusy(true);

  try{
    await openCellsForBatch(b.id);

    extractionStage = 'extracting';

    document.getElementById('stepInfo').textContent =
      'Шаг 1 из 3: дождитесь открытия ячеек и извлеките ОП и ОПТ.';

    setExtractStatus(
      'Ячейки открыты. После извлечения ОП и ОПТ нажмите «Извлечение выполнено».',
      'ok'
    );

    document.getElementById('stepActions').innerHTML = `
      <button class="btn" type="button" onclick="toExtractionDoorCloseStep()">
        Извлечение выполнено
      </button>
      <button class="btn" type="button" onclick="reopenExtractionCells()">
        Открыть ячейки ещё раз
      </button>
    `;

  }catch(e){
    extractionStage = 'opening_error';

    setExtractStatus(
      apiErrorMessage(
        e,
        'Не удалось открыть ячейки для извлечения.'
      ),
      'error'
    );

    document.getElementById('stepActions').innerHTML = `
      <button class="btn" type="button" onclick="retryExtractionOpening()">
        Повторить открытие
      </button>
    `;
  }finally{
    setExtractionBusy(false);
  }
}


async function retryExtractionOpening(){
  if(!currentBatch || extractionBusy) return;

  extractionStage = 'opening';

  setExtractStatus(
    'Повторная отправка команд открытия…',
    'waiting'
  );

  setExtractionBusy(true);

  try{
    await openCellsForBatch(currentBatch.id);

    extractionStage = 'extracting';

    document.getElementById('stepInfo').textContent =
      'Шаг 1 из 3: дождитесь открытия ячеек и извлеките ОП и ОПТ.';

    setExtractStatus(
      'Ячейки открыты. После извлечения ОП и ОПТ нажмите «Извлечение выполнено».',
      'ok'
    );

    document.getElementById('stepActions').innerHTML = `
      <button class="btn" type="button" onclick="toExtractionDoorCloseStep()">
        Извлечение выполнено
      </button>
      <button class="btn" type="button" onclick="reopenExtractionCells()">
        Открыть ячейки ещё раз
      </button>
    `;

  }catch(e){
    extractionStage = 'opening_error';

    setExtractStatus(
      apiErrorMessage(
        e,
        'Не удалось открыть ячейки для извлечения.'
      ),
      'error'
    );
  }finally{
    setExtractionBusy(false);
  }
}


async function reopenExtractionCells(){
  if(!currentBatch || extractionBusy) return;

  setExtractStatus(
    'Повторное открытие ячеек…',
    'waiting'
  );

  setExtractionBusy(true);

  try{
    await openCellsForBatch(currentBatch.id);

    setExtractStatus(
      'Команды повторного открытия выполнены.',
      'ok'
    );
  }catch(e){
    setExtractStatus(
      apiErrorMessage(
        e,
        'Не удалось повторно открыть ячейки.'
      ),
      'error'
    );
  }finally{
    setExtractionBusy(false);
  }
}


function toExtractionDoorCloseStep(){
  if(!currentBatch || extractionBusy) return;

  extractionStage = 'closing_doors';

  document.getElementById('stepInfo').textContent =
    'Шаг 2 из 3: закройте все открытые ячейки загрузки и выгрузки. Переход к печати разрешён только после подтверждённого закрытия дверей.';

  document.getElementById('docs').innerHTML = '';

  setExtractStatus(
    'Закройте все ячейки партии, затем нажмите «Проверить закрытие».',
    'waiting'
  );

  document.getElementById('stepActions').innerHTML = `
    <button class="btn" type="button" onclick="confirmExtractionDoorsClosed()">
      Проверить закрытие
    </button>
    <button class="btn" type="button" onclick="reopenExtractionCells()">
      Открыть ячейки ещё раз
    </button>
  `;
}


async function confirmExtractionDoorsClosed(){
  if(!currentBatch || extractionBusy) return;

  setExtractStatus(
    'Проверяется фактическое состояние дверей…',
    'waiting'
  );

  setExtractionBusy(true);

  try{
    await sendCommandAndWait(
      'CHECK_BATCH_EXTRACTION_DOORS',
      {
        batch_id: Number(currentBatch.id),
        purpose: 'extraction'
      },
      20000
    );

    extractionStage = 'printing';

    document.getElementById('stepInfo').textContent =
      'Шаг 3 из 3: выберите документы и нажмите «Печать и завершить». По умолчанию выбраны все документы.';

    document.getElementById('docs').innerHTML = `
      <label><input type="checkbox" value="summary" checked> Протокол партии</label><br/>
      <label><input type="checkbox" value="label" checked> Ярлыки</label><br/>
      <label><input type="checkbox" value="defect_protocol" checked> Протокол брака</label><br/>
    `;

    setExtractStatus(
      'Все двери закрыты. Можно печатать документы и завершать извлечение.',
      'ok'
    );

    document.getElementById('stepActions').innerHTML = `
      <button class="btn" type="button" onclick="printAndFinishExtraction()">
        Печать и завершить
      </button>
    `;

  }catch(e){
    extractionStage = 'closing_doors';

    setExtractStatus(
      apiErrorMessage(
        e,
        'Не удалось подтвердить закрытие всех ячеек.'
      ),
      'error'
    );
  }finally{
    setExtractionBusy(false);
  }
}


async function printAndFinishExtraction(){
  if(!currentBatch || extractionBusy) return;

  const box = document.getElementById('docs');
  const selected = Array.from(
    box?.querySelectorAll('input[type="checkbox"]:checked') || []
  )
    .map(input => String(input.value || '').trim())
    .filter(Boolean);

  if(!extractionDocsPrinted && selected.length === 0){
    setExtractStatus(
      'Выберите хотя бы один документ для печати.',
      'error'
    );
    return;
  }

  setExtractionBusy(true);

  try{
    if(!extractionDocsPrinted){
      setExtractStatus(
        'Документы отправляются на печать…',
        'waiting'
      );

      await sendCommandAndWait(
        'PRINT_DOCS',
        {
          batch_id: Number(currentBatch.id),
          docs: selected,
          purpose: 'extraction'
        },
        60000
      );

      extractionDocsPrinted = true;

      if(box){
        box
          .querySelectorAll('input[type="checkbox"]')
          .forEach(input => {
            input.disabled = true;
          });
      }
    }

    setExtractStatus(
      'Печать выполнена. Повторная проверка закрытия дверей и освобождение ячеек…',
      'waiting'
    );

    await sendCommandAndWait(
      'CHECK_BATCH_EXTRACTION_DOORS',
      {
        batch_id: Number(currentBatch.id),
        purpose: 'extraction'
      },
      20000
    );

    const extractedBatchId = Number(currentBatch.id);

    await sendCommandAndWait(
      'MARK_ACTIVE_BATCH_EXTRACTED',
      {
        batch_id: extractedBatchId,
        purpose: 'extraction'
      },
      20000
    );

    currentBatch = null;
    extractionStage = 'complete';

    document.getElementById('stepInfo').textContent =
      'Извлечение партии завершено.';

    document.getElementById('docs').innerHTML = '';

    setExtractStatus(
      `Партия #${extractedBatchId} извлечена, документы отправлены на печать, ячейки освобождены.`,
      'ok'
    );

    document.getElementById('stepActions').innerHTML = `
      <button class="btn" type="button" onclick="finishAndCloseExtractModal()">
        Закрыть
      </button>
    `;

    await loadBatches();

  }catch(e){
    setExtractStatus(
      apiErrorMessage(
        e,
        extractionDocsPrinted
          ? 'Документы напечатаны, но не удалось завершить извлечение. Повторная печать при повторе выполняться не будет.'
          : 'Не удалось напечатать документы и завершить извлечение.'
      ),
      'error'
    );

    document.getElementById('stepActions').innerHTML = `
      <button class="btn" type="button" onclick="printAndFinishExtraction()">
        ${extractionDocsPrinted ? 'Повторить завершение' : 'Повторить печать и завершение'}
      </button>
    `;
  }finally{
    setExtractionBusy(false);
  }
}


function renderCells(){
  const mk = (sideElId, n, kind) => {
    const el = document.getElementById(sideElId);
    el.innerHTML = '';
    for(let i=1;i<=n;i++){
      const b = findBatchByCell(i, kind);

      const data = (b?.data||{});
      const passNo = data.passport_number || data.product_code || '-';
      const cnt = data.product_count ?? '-';

      const good = Number(data.measured_good ?? 0);
      const bad  = Number(data.measured_bad ?? 0);
      const meas = (good + bad) || Number(data.measured_qty ?? 0);

      const stLbl = b
        ? (
            data.stop_cleanup_active === true
              ? 'останов.'
              : cellStatusLabel(b.status)
          )
        : '';

      const div = document.createElement('div');
      div.className = cellClass(b);

      div.innerHTML = `
        <div>
          <div class="cellHead">
            <div class="n">${i}</div>

            ${
              b
                ? (
                    `<span
                      class="pill"
                      title="${String(b.status || '')}"
                    >${stLbl}</span>`
                  )
                : ''
            }
          </div>

          <div class="meta">
            ${
              b
                ? (
                    `<div>#${b.id} · ${passNo}</div>` +
                    `<div>· ${meas}/${cnt}</div>`
                  )
                : '<span class="muted">пусто</span>'
            }
          </div>
        </div>

        <div class="actions">
          ${
            kind === 'loading'
              ? (
                  b
                    ? ''
                    : (
                        `<button
                          class="btn sm"
                          onclick="openNewBatchModal(${i})"
                        >+партия</button>`
                      )
                )
              : (
                  canExtractBatch(b)
                    ? (
                        `<button
                          class="btn sm"
                          onclick="startExtractByBatchId(${b.id})"
                        >Извлечь</button>`
                      )
                    : ''
                )
          }
        </div>
      `;

      el.appendChild(div);
    }
  };

  mk('cellsLoading', LOADING_COUNT, 'loading');
  mk('cellsUnloading', UNLOADING_COUNT, 'unloading');
}


function renderActiveProgress(){
  const st = stateCache || {};
  const abId = st.active_batch_id;

  const isAuto = st.mode === 'auto_running';

  if(isAuto && abId){
    if(autoStartedBatchId !== abId){
      autoStartedBatchId = abId;

      const b0 = (batchesCache || []).find(
        x => Number(x.id) === Number(abId)
      );

      const startedRaw = (
        st.active_operation_started_at ||
        b0?.data?.started_ts
      );

      const parsed = parseServerTimestampMs(startedRaw);
      const nowMs = Date.now();

      autoStartedAtMs = (
        Number.isFinite(parsed) &&
        parsed <= nowMs + 1000
      )
        ? parsed
        : nowMs;
    }
  }else{
    autoStartedAtMs = null;
    autoStartedBatchId = null;
  }

  const timeEl = document.getElementById('ab_time');

  if(timeEl){
    timeEl.textContent = autoStartedAtMs
      ? fmtDur(Date.now() - autoStartedAtMs)
      : '-';
  }

  if(!abId){
    ab.textContent = '-';
    ab_prog.textContent = '-';
    lm_ok.textContent = '-';
    lm_items.innerHTML = '<div class="muted">-</div>';
    return;
  }

  const b = (batchesCache || []).find(
    x => Number(x.id) === Number(abId)
  );

  if(!b){
    ab.textContent = '-';
    ab_prog.textContent = '?';
    lm_ok.textContent = '-';
    return;
  }

  const data = b.data || {};

  const passNo = (
    data.passport_number ||
    data.product_code ||
    '-'
  );

  const good = Number(
    data.measured_good ?? 0
  );

  const bad = Number(
    data.measured_bad ?? 0
  );

  const measured = (
    (good + bad) ||
    Number(data.measured_qty ?? 0)
  );

  const expected = Number(
    data.product_count || 0
  );

  // В верхней строке текущей партии показываем только паспорт.
  ab.textContent = passNo;

  ab_prog.textContent = expected > 0
    ? `${measured}/${expected}`
    : String(measured);

  lm_ok.textContent = `${good} / ${bad}`;
}


function renderLastMeas(){
  const st = stateCache || {};

  if(!st.active_batch_id){
    lm_items.innerHTML = '<div class="muted">-</div>';
    return;
  }

  const s = st.last_meas_summary;
  const items = Array.isArray(s?.items) ? s.items : [];

  if(items.length === 0){
    lm_items.innerHTML = '<div class="muted">нет данных</div>';
    return;
  }

  const fmtVal = (v) => {
    if(v === null || v === undefined) return '-';

    const n = Number(v);

    if(Number.isFinite(n)){
      return n.toFixed(4);
    }

    return String(v);
  };

  const isZeroValue = (value) => {
    /*
     * null, undefined и пустую строку не считаем нулём,
     * чтобы отсутствие данных не определялось как
     * некорректное измерение.
     */
    if(
      value === null ||
      value === undefined ||
      String(value).trim() === ''
    ){
      return false;
    }

    const n = Number(value);

    return Number.isFinite(n) && n === 0;
  };

  /*
   * Проверку выполняем первой, поскольку при некорректном
   * измерении признаки отдельных размеров могут быть любыми,
   * но все значения от ИМ будут равны нулю.
   */
  const allValuesZero = items.every(
    item => isZeroValue(item?.value)
  );

  if(allValuesZero){
    lm_items.innerHTML = '<div class="bad">НЕ ИЗМЕРЕНА</div>';
    return;
  }

  /*
   * Деталь считается годной, когда каждый размер имеет
   * явный признак OK.
   */
  const allItemsOk = items.every(
    item => item?.ok === true
  );

  if(allItemsOk){
    lm_items.innerHTML = '<div class="ok">ГОДНАЯ</div>';
    return;
  }

  /*
   * Для бракованной детали оставляем только размеры,
   * которые имеют явный признак NOK.
   */
  const nokItems = items.filter(
    item => item?.ok === false
  );

  if(nokItems.length === 0){
    lm_items.innerHTML = '<div class="muted">нет данных</div>';
    return;
  }

  lm_items.innerHTML = nokItems.map(item => {
    const valTxt = fmtVal(item.value);

    return `
      <div class="badMeasField">
        <span class="mono">
          ${item.name ?? '-'}
        </span>

        <b class="mono">
          ${valTxt}
        </b>
      </div>
    `;
  }).join('');
}


function scheduleLiveRefresh(){
  liveRefreshPending = true;

  if(
    liveRefreshTimerId !== null ||
    liveRefreshInFlight
  ){
    return;
  }

  liveRefreshTimerId = setTimeout(()=>{
    liveRefreshTimerId = null;
    runLiveRefresh();
  }, LIVE_REFRESH_DEBOUNCE_MS);
}


async function runLiveRefresh(){
  if(liveRefreshInFlight){
    liveRefreshPending = true;
    return;
  }

  liveRefreshInFlight = true;
  liveRefreshPending = false;

  try{
    await Promise.all([
      loadState(),
      loadBatches()
    ]);
  }catch(e){
    console.error('live refresh failed', e);
  }finally{
    liveRefreshInFlight = false;

    if(liveRefreshPending){
      scheduleLiveRefresh();
    }
  }
}


function startSse(){
  if(es){ try{ es.close(); }catch(e){} }
  conn.textContent = 'SSE: connecting';
  es = new EventSource('events');
  es.onopen = ()=>{
    conn.textContent = 'SSE: connected';

    // EventSource сам восстанавливает поток по Last-Event-ID. После любого
    // подключения дополнительно перечитываем актуальный state, чтобы UI
    // синхронизировался даже при реконнекте без нового события.
    scheduleLiveRefresh();
  };
  es.onerror = ()=>{
    conn.textContent = 'SSE: error (reconnect)';
  };
  es.onmessage = scheduleLiveRefresh;
}


(async function init(){
  renderCurrentDateTime();
  await loadWorkplaceUser();

  await refreshAll();
  startSse();

  // Текущие дата и время.
  setInterval(()=>{
    try{
      renderCurrentDateTime();
    }catch(e){}
  }, 1000);

  // Секундный отсчёт времени разбора партии.
  setInterval(()=>{
    try{
      renderActiveProgress();
    }catch(e){}
  }, 1000);

  // Температуры, подключения роботов и номер тары брака.
  setInterval(()=>{
    try{
      loadTemps();
    }catch(e){}
  }, 1000);

})();
</script>

<div id="nbModal" class="modal hidden" onclick="nbBackdropClick(event)">
  <div class="modalCard">
    <div class="modalHead">
      <div class="nbTitleControls">
        <b id="nb_title">Новая партия. Ячейка -</b>

        <button
          id="nb_qr_scan_btn"
          class="btn"
          type="button"
          onclick="startQrScan()"
        >
          Сканировать QR
        </button>
      </div>

      <button class="btn sm" onclick="closeNewBatchModal()">Закрыть</button>
    </div>

    <div
      id="nb_qr_status"
      class="qrStatus empty"
      aria-live="polite"
    ></div>

    <div class="batchFormGrid" style="margin-top:6px">
      <div class="field"><small class="muted">Маршрутный паспорт</small><input id="nb_passport_number" class="inp" required /></div>
      <div class="field"><small class="muted">Наименование детали</small><input id="nb_product_name" class="inp" required /></div>
      <div class="field"><small class="muted">Количество</small><input id="nb_product_count" class="inp" type="number" min="1" required /></div>

      <div class="field"><small class="muted">Дата маршрутного паспорта</small><input id="nb_passport_date" class="inp" type="date" required /></div>
      <div class="field"><small class="muted">Обозначение детали</small><input id="nb_product_code" class="inp" required /></div>
      <div class="field"><small class="muted">Содержание железа</small><input id="nb_prod_fe" class="inp" required /></div>

      <div class="field"><small class="muted">Сплав</small><input id="nb_blank_alloy" class="inp" required /></div>
      <div class="field"><small class="muted">Номер изменения чертежа</small><input id="nb_draw_rev" class="inp" required /></div>
      <div class="field"><small class="muted">Доп параметр1</small><input id="nb_prod_opt1" class="inp" /></div>

      <div class="field"><small class="muted">Заготовка</small><input id="nb_blank_name" class="inp" required /></div>
      <div class="field"><small class="muted">Номер сертификата</small><input id="nb_cert_number" class="inp" required /></div>
      <div class="field"><small class="muted">Доп параметр2</small><input id="nb_prod_opt2" class="inp" /></div>

      <div class="field"><small class="muted">ТУ на слиток</small><input id="nb_prod_tsi" class="inp" required /></div>
      <div class="field"><small class="muted">Номер партии прутка</small><input id="nb_rod_batch_number" class="inp" required /></div>
      <div class="field"><small class="muted">Доп параметр3</small><input id="nb_prod_opt3" class="inp" /></div>

      <div class="field"><small class="muted">ТУ на заготовку</small><input id="nb_prod_tsb" class="inp" required /></div>
      <div class="field"><small class="muted">Масса, кг</small><input id="nb_items_mass" class="inp" type="number" min="0.001" step="0.001" required /></div>
      <div class="field"><small class="muted">Доп параметр4</small><input id="nb_prod_opt4" class="inp" /></div>

      <div class="field fieldWide"><small class="muted">Комментарий</small><input id="nb_prod_comment" class="inp" /></div>
    </div>

    <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap">
      <button
        class="btn"
        type="button"
        onclick="submitNewBatch()"
      >
        Создать партию
      </button>

      <button
        class="btn"
        type="button"
        onclick="clearNewBatch()"
      >
        Очистить
      </button>
    </div>

  </div>
</div>

<div
  id="rbTareModal"
  class="modal hidden"
  role="dialog"
  aria-modal="true"
  aria-labelledby="rbTareModalTitle"
>
  <div class="modalCard">
    <div class="modalHead">
      <div><b id="rbTareModalTitle">Установка тары брака</b></div>
    </div>

    <div style="margin-top:12px">
      <div class="field">
        <small class="muted">Введите номер устанавливаемой тары от 1 до 10</small>

        <input
          id="rb_tare_no"
          class="inp"
          type="number"
          min="1"
          max="10"
          step="1"
          inputmode="numeric"
          onkeydown="
            if(event.key === 'Enter'){
              event.preventDefault();
              submitRejectBinTare();
            }
          "
        />
      </div>

      <div id="rb_tare_status" class="qrStatus empty" aria-live="polite"></div>

      <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap">
        <button id="rb_tare_submit" class="btn" type="button" onclick="submitRejectBinTare()">
          Подтвердить номер
        </button>
      </div>
    </div>
  </div>
</div>

<div id="exModal" class="modal hidden" onclick="exBackdropClick(event)">
  <div class="modalCard">
    <div class="modalHead">
      <div><b id="exTitle">Извлечение партии</b></div>
      <button id="exCloseBtn" class="btn sm" type="button" onclick="closeExtractModal()">Закрыть</button>
    </div>

	<div id="ex_status" class="qrStatus empty" aria-live="polite"></div>
    <div style="margin-top:10px">
      <div class="muted" id="stepTitle"></div>
      <div class="muted" id="stepInfo"></div>    
      <div id="docs" style="margin-top:10px"></div>
      <div id="stepActions" style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap"></div>
    </div>
  </div>
</div>

</body>
</html>
        """.strip())


@router.get("/ui", response_class=HTMLResponse)
def debug_ui(
    current_user: UserRow | None = Depends(get_current_user_optional),
):
    """Страница настроек и тестовых команд IM/RTK."""
    if current_user is None:
        return RedirectResponse(url="/api/login", status_code=303)
    if str(current_user.role) != UserRole.admin.value:
        raise HTTPException(
            status_code=403,
            detail="Действие доступно только администратору.",
        )

    html = """<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Настройки и диагностика</title>
  <style>
    :root{
      font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
      color:#273142;
      background:#fff;
    }

    *{box-sizing:border-box}
    body{margin:0;background:#fff}

    .statusBar{
      min-height:48px;
      padding:8px 12px;
      border-bottom:1px solid #e5e7eb;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      flex-wrap:wrap;
      background:#fff;
    }

    .headerLeft,
    .headerRight{
      display:flex;
      align-items:center;
      gap:4px;
      flex-wrap:wrap;
    }

    .clockText{
      min-width:153px;
      font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
      font-size:13px;
      color:#374151;
      white-space:nowrap;
    }

    .statusField{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-height:23px;
      padding:1px 3px;
      border:1px solid #d1d5db;
      border-radius:0;
      font-size:13px;
      font-weight:600;
      white-space:nowrap;
    }

    .statusField.statusOk{
      color:#166534;
      background:#dcfce7;
      border-color:#86efac;
    }

    .statusField.statusBad{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
    }

    .statusField.statusUnknown{
      color:#6b7280;
      background:#f3f4f6;
      border-color:#d1d5db;
    }

    .separator{
      color:#d1d5db;
      user-select:none;
    }

    .tareNumber{
      display:inline-flex;
      align-items:center;
      gap:2px;
      color:#4b5563;
      white-space:nowrap;
    }

    .rejectFill,
    .temperatureField{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      min-height:23px;
      padding:1px 3px;
      border:1px solid #d1d5db;
      border-radius:0;
      font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
      font-size:13px;
      font-weight:700;
      white-space:nowrap;
    }

    .rejectFill{min-width:52px}

    .rejectFill.rejectNormal,
    .temperatureField.tempOk{
      color:#166534;
      background:#dcfce7;
      border-color:#86efac;
    }

    .rejectFill.rejectNear,
    .temperatureField.tempNear{
      color:#9a3412;
      background:#ffedd5;
      border-color:#fdba74;
    }

    .rejectFill.rejectFull,
    .temperatureField.tempBad{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
    }

    .rejectFill.rejectUnknown,
    .temperatureField.tempUnknown{
      color:#6b7280;
      background:#f3f4f6;
      border-color:#d1d5db;
    }

    .userArea{
      display:flex;
      align-items:center;
      gap:8px;
      color:#6b7280;
      font-size:13px;
      white-space:nowrap;
    }

    .userNameLink{
      color:#174a8b;
      font-weight:700;
      text-decoration:underline;
      text-underline-offset:2px;
      cursor:pointer;
    }

    .logoutBtn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      width:30px;
      height:30px;
      padding:0;
      border:0;
      background:transparent;
      color:#000;
    }

    .logoutBtn:disabled{
      opacity:.55;
      cursor:not-allowed;
    }

    .soundIconBtn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      width:26px;
      height:23px;
      min-width:26px;
      padding:0;
      border-radius:0;
      font-size:16px;
      line-height:1;
    }

    .soundIconBtn.soundMuted{
      color:#991b1b;
      background:#fee2e2;
      border-color:#fca5a5;
      font-weight:700;
    }

    .pageGrid{
      display:grid;
      grid-template-columns:minmax(340px, 470px) minmax(0, 1fr);
      gap:12px;
      align-items:start;
      padding:12px;
    }

    .leftColumn,
    .rightColumn{
      display:flex;
      flex-direction:column;
      gap:12px;
      min-width:0;
    }

    .card{
      min-width:0;
      padding:12px;
      border:1px solid #ddd;
      border-radius:12px;
      background:#fff;
      box-shadow:0 1px 2px rgba(15,23,42,.03);
    }

    .cardTitle{
      margin:0 0 10px;
      font-size:16px;
      font-weight:700;
    }

    .cardHead{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      flex-wrap:wrap;
      margin-bottom:10px;
    }

    .cardHead .cardTitle{margin:0}

    .row{
      display:flex;
      gap:8px;
      flex-wrap:wrap;
      align-items:center;
    }

    .fields{
      display:flex;
      flex-direction:column;
      gap:10px;
    }

    .field{
      display:flex;
      flex-direction:column;
      gap:4px;
      min-width:0;
    }

    .fieldLabel{
      color:#4b5563;
      font-size:13px;
    }

    .checkboxField{
      min-height:38px;
      flex-direction:row;
      align-items:center;
      justify-content:space-between;
      gap:12px;
    }

    input,select,textarea{
      padding:8px;
      border:1px solid #bbb;
      border-radius:10px;
      background:#fff;
      color:#273142;
    }

    input[type=\"number\"],
    input[type=\"text\"],
    input[type=\"password\"]{
      width:100%;
    }

    input[type=\"checkbox\"]{
      width:18px;
      height:18px;
      margin:0;
    }

    button{
      padding:8px 10px;
      border:1px solid #bbb;
      border-radius:10px;
      background:#fff;
      cursor:pointer;
    }

    button:hover{border-color:#777}
    button:disabled{opacity:.5;cursor:not-allowed}

    .operatorsList{
      display:flex;
      flex-direction:column;
      border:1px solid #e5e7eb;
      border-radius:10px;
      overflow:hidden;
    }

    .operatorRow{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      padding:8px 10px;
      background:#fff;
    }

    .operatorRow + .operatorRow{
      border-top:1px solid #e5e7eb;
    }

    .operatorName{
      min-width:0;
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
      font-size:14px;
    }

    .operatorEmpty{
      padding:10px;
      color:#6b7280;
      font-size:13px;
    }

    .operatorForm{
      margin-top:10px;
      padding-top:10px;
      border-top:1px solid #e5e7eb;
    }

    .operatorForm.hidden{display:none}

    .dangerBtn{
      color:#991b1b;
      border-color:#fecaca;
      background:#fff;
    }

    .dangerBtn:hover{
      border-color:#f87171;
    }

    .muted{
      color:#666;
      font-size:12px;
    }

    .hint{
      min-height:18px;
      margin-top:10px;
      font-size:12px;
      color:#666;
    }

    .hint.ok{color:#166534}
    .hint.error{color:#991b1b}
    .hint.waiting{color:#075985}

    pre{
      margin:0;
      white-space:pre-wrap;
      word-break:break-word;
      font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
      font-size:12px;
    }


    @media (max-width:900px){
      .pageGrid{grid-template-columns:1fr}
    }

    @media (max-width:620px){
      .clockText{
        min-width:0;
        width:100%;
      }
    }
  </style>
</head>
<body>
  <div class=\"statusBar\">
    <div class=\"headerLeft\">
      <div id=\"currentDateTime\" class=\"clockText\">--.--.---- --:--:--</div>

      <span id=\"safety\" class=\"statusField statusUnknown\">АвКонтур</span>
      <span class=\"separator\">|</span>

      <span id=\"air_pressure\" class=\"statusField statusUnknown\">Воздух</span>
      <span class=\"separator\">|</span>

      <span id=\"trash\" class=\"statusField statusUnknown\">Тара брака</span>
      <span class=\"tareNumber\">№<b id=\"reject_tare_no\">-</b></span>
      <span id=\"reject\" class=\"rejectFill rejectUnknown\">-</span>
      <span class=\"separator\">|</span>

      <span id=\"temp_im_field\" class=\"temperatureField tempUnknown\">
        tм:&nbsp;<b id=\"temp_im\">-</b><span id=\"temp_im_unit\"></span>
      </span>

      <span id=\"temp_loading_field\" class=\"temperatureField tempUnknown\">
        tп:&nbsp;<b id=\"temp_loading\">-</b><span id=\"temp_loading_unit\"></span>
      </span>

      <span class=\"separator\">|</span>

      <button
        id=\"adminEmergencySoundBtn\"
        class=\"soundIconBtn\"
        type=\"button\"
        onclick=\"toggleAdminEmergencySound()\"
        aria-pressed=\"false\"
        aria-label=\"Отключить аварийный звук\"
        title=\"Аварийный звук сейчас не активен\"
        disabled
      >🔊</button>
    </div>

    <div class=\"headerRight\">
      <div class=\"userArea\">
        <a
          id=\"headerUser\"
          class=\"userNameLink\"
          href=\"/api/workplace\"
          title=\"Вернуться в рабочий интерфейс\"
        >—</a>
        <button
          class=\"logoutBtn\"
          type=\"button\"
          onclick=\"logoutAdmin()\"
          title=\"Выйти\"
          aria-label=\"Выйти из системы\"
        >
          <svg
            width=\"18\"
            height=\"18\"
            viewBox=\"0 0 24 24\"
            fill=\"none\"
            stroke=\"currentColor\"
            stroke-width=\"1.8\"
            stroke-linecap=\"round\"
            stroke-linejoin=\"round\"
            aria-hidden=\"true\"
          >
            <path d=\"M10 17l5-5-5-5\"/>
            <path d=\"M15 12H3\"/>
            <path d=\"M14 3h5a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-5\"/>
          </svg>
        </button>
      </div>
    </div>
  </div>

  <main class=\"pageGrid\">
    <div class=\"leftColumn\">
      <section class=\"card\">
        <div class=\"cardHead\">
          <h2 class=\"cardTitle\">Настройки</h2>
          <div class=\"row\">
            <button type=\"button\" onclick=\"loadSettings()\">Загрузить</button>
            <button type=\"button\" onclick=\"saveSettings()\">Сохранить</button>
          </div>
        </div>

        <div class=\"fields\">
          <label class=\"field\">
            <span class=\"fieldLabel\">Допустимая доля брака [0..1]</span>
            <input
              id=\"set_ppod\"
              type=\"number\"
              min=\"0\"
              max=\"1\"
              step=\"0.01\"
            />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Количество браков подряд до проверки</span>
            <input
              id=\"set_consecutive_rejects_threshold\"
              type=\"number\"
              min=\"1\"
              step=\"1\"
            />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Кол-во ячеек в таре брака</span>
            <input id=\"set_reject_cap\" type=\"number\" min=\"0\" step=\"1\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Предупреждать за N ячеек</span>
            <input id=\"set_rjb_near_full\" type=\"number\" min=\"0\" step=\"1\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Мин. допустимая температура на постаматах (°C)</span>
            <input id=\"set_t1_min\" type=\"number\" step=\"0.1\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Макс. допустимая температура на постаматах (°C)</span>
            <input id=\"set_t1_max\" type=\"number\" step=\"0.1\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Мин. допустимая температура на микрометре (°C)</span>
            <input id=\"set_t2_min\" type=\"number\" step=\"0.1\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Макс. допустимая температура на микрометре (°C)</span>
            <input id=\"set_t2_max\" type=\"number\" step=\"0.1\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Предупреждение за N °C</span>
            <input id=\"set_temp_near_critical\" type=\"number\" min=\"0\" step=\"0.5\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Раскладка</span>
            <select id=\"set_layout\">
              <option value=\"0\">Раскладка А</option>
              <option value=\"1\">Раскладка Б</option>
              <option value=\"2\">Раскладка В</option>
              <option value=\"3\">Раскладка Г</option>
            </select>
          </label>

          <label class=\"field checkboxField\">
            <span class=\"fieldLabel\">Противоволна в ОП</span>
            <input id=\"set_use_alternate_wave\" type=\"checkbox\" />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">IP-адрес UDP-сервера</span>
            <input
              id=\"set_udp_server_ip\"
              type=\"text\"
              inputmode=\"text\"
              autocomplete=\"off\"
              spellcheck=\"false\"

            />
          </label>

          <label class=\"field\">
            <span class=\"fieldLabel\">Порт UDP-сервера</span>
            <input
              id=\"set_udp_server_port\"
              type=\"number\"
              min=\"1\"
              max=\"65535\"
              step=\"1\"
              placeholder=\"9999\"
            />
          </label>
        </div>

        <div id=\"set_hint\" class=\"hint\">—</div>
      </section>
    </div>

    <div class=\"rightColumn\">
      <section class=\"card\">
        <div class=\"cardHead\">
          <h2 class=\"cardTitle\">Операторы</h2>
          <button type=\"button\" onclick=\"showOperatorForm()\">+ Добавить оператора</button>
        </div>

        <div id=\"operatorsList\" class=\"operatorsList\">
          <div class=\"operatorEmpty\">Загрузка…</div>
        </div>

        <div id=\"operatorForm\" class=\"operatorForm hidden\">
          <div class=\"fields\">
            <label class=\"field\">
              <span class=\"fieldLabel\">Фамилия</span>
              <input
                id=\"operatorLastName\"
                type=\"text\"
                maxlength=\"64\"
                autocomplete=\"family-name\"
              />
            </label>

            <label class=\"field\">
              <span class=\"fieldLabel\">Имя</span>
              <input
                id=\"operatorFirstName\"
                type=\"text\"
                maxlength=\"64\"
                autocomplete=\"given-name\"
              />
            </label>

            <label class=\"field\">
              <span class=\"fieldLabel\">Отчество</span>
              <input
                id=\"operatorMiddleName\"
                type=\"text\"
                maxlength=\"64\"
                autocomplete=\"additional-name\"
              />
            </label>

            <label class=\"field\">
              <span class=\"fieldLabel\">Пароль</span>
              <input
                id=\"operatorPassword\"
                type=\"password\"
                maxlength=\"1024\"
                autocomplete=\"new-password\"
              />
            </label>

            <label class=\"field\">
              <span class=\"fieldLabel\">Повтор пароля</span>
              <input
                id=\"operatorPasswordConfirm\"
                type=\"password\"
                maxlength=\"1024\"
                autocomplete=\"new-password\"
              />
            </label>
          </div>

          <div class=\"row\" style=\"margin-top:10px\">
            <button type=\"button\" onclick=\"createOperator()\">Создать</button>
            <button type=\"button\" onclick=\"hideOperatorForm()\">Отмена</button>
          </div>
        </div>

        <div id=\"operators_hint\" class=\"hint\"></div>
      </section>

      <section class=\"card\">
        <div class=\"cardHead\">
          <h2 class=\"cardTitle\">Экспорт данных на USB</h2>
          <button type=\"button\" onclick=\"loadExportDrives()\">Обновить</button>
        </div>

        <div class=\"fields\">
          <label class=\"field\">
            <span class=\"fieldLabel\">USB-носитель</span>
            <select id=\"exportDrive\" style=\"width:100%\">
              <option value=\"\">Поиск носителей…</option>
            </select>
          </label>

          <div id=\"exportDriveInfo\" class=\"muted\">—</div>

          <div class=\"row\">
            <button id=\"exportStartBtn\" type=\"button\" onclick=\"startUsbExport()\">
              Экспортировать
            </button>
            <button id=\"exportUnmountBtn\" type=\"button\" onclick=\"safeUnmountUsb()\">
              Безопасно извлечь
            </button>
          </div>
        </div>

        <div id=\"export_hint\" class=\"hint\"></div>
      </section>

      <section class=\"card\">
        <h2 class=\"cardTitle\">РТК - сервисные команды</h2>

        <div class=\"row\">
          <button type=\"button\" onclick=\"sendCmd('RTK_RESET')\">СБРОС</button>
          <button type=\"button\" onclick=\"sendCmd('RTK_PAUSE')\">ПАУЗА</button>
          <button type=\"button\" onclick=\"sendCmd('RTK_RESUME')\">ПРОДОЛЖИТЬ</button>
        </div>

        <div class=\"row\" style=\"margin-top:8px\">
          <input id=\"rtkSpeed\" type=\"number\" min=\"0\" max=\"100\" step=\"1\" placeholder=\"speed (0..100)\" style=\"width:160px\" />
          <button type=\"button\" onclick=\"sendCmd('RTK_SETSPEED',{speed:Number(val('rtkSpeed')||0)})\">Скорость РТК</button>
        </div>

        <div id=\"rtk_hint\" class=\"hint\"></div>
      </section>

      <section class=\"card\">
        <h2 class=\"cardTitle\">Микрометр - сервисные команды</h2>

        <div class=\"row\">
          <button type=\"button\" onclick=\"sendCmd('IM_VACUUM_ON')\">Вакуум Вкл.</button>
          <button type=\"button\" onclick=\"sendCmd('IM_VACUUM_OFF')\">Вакуум Выкл.</button>
          <button type=\"button\" onclick=\"sendCmd('IM_CALIBRATE')\">Калибровка</button>
        </div>

        <div class=\"row\" style=\"margin-top:8px\">
          <input id=\"imPc\" type=\"text\" placeholder=\"312.229.002\" style=\"flex:1;min-width:220px\" />
          <button type=\"button\" onclick=\"sendCmd('IM_LOAD_PROGRAM',{product_code: val('imPc')})\">Загрузить ИП</button>
          <button type=\"button\" onclick=\"sendCmd('IM_MEASURE_ONCE',{product_code: val('imPc')})\">Измерить</button>
        </div>

        <div id=\"im_hint\" class=\"hint\"></div>
      </section>
    </div>
  </main>

<script>
  const API_BASE = location.pathname.replace(/\\/ui\\/?$/, '');

  let stateCache = null;
  let runtimeSettingsCache = null;
  let adminEmergencySoundToggleInFlight = false;

  function val(id){
    return (document.getElementById(id)?.value || '').trim();
  }

  function pad2(value){
    return String(value).padStart(2, '0');
  }

  function renderCurrentDateTime(){
    const el = document.getElementById('currentDateTime');
    if(!el) return;

    const now = new Date();
    el.textContent =
      `${pad2(now.getDate())}.` +
      `${pad2(now.getMonth() + 1)}.` +
      `${now.getFullYear()} ` +
      `${pad2(now.getHours())}:` +
      `${pad2(now.getMinutes())}:` +
      `${pad2(now.getSeconds())}`;
  }

  async function loadCurrentUser(){
    const header = document.getElementById('headerUser');
    if(!header) return;

    try{
      const user = await fetchJson(`${API_BASE}/auth/me`);
      const value = String(user?.display_name || '').trim();
      header.textContent = value || '—';
    }catch(e){
      header.textContent = 'Не авторизован';
    }
  }

  window.logoutAdmin = async function(){
    try{
      await fetchJson(`${API_BASE}/auth/logout`, {method:'POST'});
    }catch(e){
      // При любом результате возвращаемся на страницу входа.
    }
    location.replace(`${API_BASE}/login`);
  };

  window.showOperatorForm = function(){
    const form = document.getElementById('operatorForm');
    if(!form) return;

    form.classList.remove('hidden');
    document.getElementById('operatorLastName').value = '';
    document.getElementById('operatorFirstName').value = '';
    document.getElementById('operatorMiddleName').value = '';
    document.getElementById('operatorPassword').value = '';
    document.getElementById('operatorPasswordConfirm').value = '';
    setHint('operators_hint', '');
    document.getElementById('operatorLastName')?.focus();
  };

  window.hideOperatorForm = function(){
    const form = document.getElementById('operatorForm');
    if(form) form.classList.add('hidden');
  };

  window.loadOperators = async function(){
    const list = document.getElementById('operatorsList');
    if(!list) return;

    list.innerHTML = '<div class="operatorEmpty">Загрузка…</div>';

    try{
      const operators = await fetchJson(`${API_BASE}/admin/operators`);
      list.innerHTML = '';

      if(!Array.isArray(operators) || operators.length === 0){
        const empty = document.createElement('div');
        empty.className = 'operatorEmpty';
        empty.textContent = 'Активных операторов нет.';
        list.appendChild(empty);
        return;
      }

      for(const operator of operators){
        const row = document.createElement('div');
        row.className = 'operatorRow';

        const name = document.createElement('div');
        name.className = 'operatorName';
        name.textContent = String(operator?.display_name || '—');
        name.title = String(operator?.full_name || name.textContent);

        const removeButton = document.createElement('button');
        removeButton.type = 'button';
        removeButton.className = 'dangerBtn';
        removeButton.textContent = 'Удалить';
        removeButton.addEventListener('click', () => {
          disableOperator(
            Number(operator?.id),
            String(operator?.display_name || '')
          );
        });

        row.appendChild(name);
        row.appendChild(removeButton);
        list.appendChild(row);
      }
    }catch(e){
      list.innerHTML = '';
      const error = document.createElement('div');
      error.className = 'operatorEmpty';
      error.textContent = `Ошибка загрузки: ${e?.message || String(e)}`;
      list.appendChild(error);
    }
  };

  window.createOperator = async function(){
    const lastName = val('operatorLastName');
    const firstName = val('operatorFirstName');
    const middleName = val('operatorMiddleName');
    const password = document.getElementById('operatorPassword')?.value || '';
    const passwordConfirm =
      document.getElementById('operatorPasswordConfirm')?.value || '';

    if(!lastName){
      setHint('operators_hint', 'Укажите фамилию оператора.', 'error');
      return;
    }
    if(!firstName){
      setHint('operators_hint', 'Укажите имя оператора.', 'error');
      return;
    }
    if(!middleName){
      setHint('operators_hint', 'Укажите отчество оператора.', 'error');
      return;
    }
    if(!password){
      setHint('operators_hint', 'Укажите пароль.', 'error');
      return;
    }
    if(password !== passwordConfirm){
      setHint('operators_hint', 'Пароли не совпадают.', 'error');
      return;
    }

    const fullName = `${lastName} ${firstName} ${middleName}`;
    setHint('operators_hint', 'Создание оператора…', 'waiting');

    try{
      const created = await fetchJson(`${API_BASE}/admin/operators`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          last_name:lastName,
          first_name:firstName,
          middle_name:middleName,
          password,
          password_confirm:passwordConfirm
        })
      });

      hideOperatorForm();
      await loadOperators();
      setHint(
        'operators_hint',
        `Оператор «${created?.display_name || fullName}» создан.`,
        'ok'
      );
    }catch(e){
      setHint(
        'operators_hint',
        `Ошибка создания: ${e?.message || String(e)}`,
        'error'
      );
    }
  };

  window.disableOperator = async function(userId, displayName){
    if(!Number.isInteger(userId) || userId <= 0) return;

    const name = String(displayName || '').trim() || `#${userId}`;
    if(!window.confirm(
      `Удалить оператора «${name}»?\n\n` +
      'Учетная запись будет деактивирована, а все ее активные сессии завершены.'
    )){
      return;
    }

    setHint('operators_hint', `Удаление оператора «${name}»…`, 'waiting');

    try{
      await fetchJson(`${API_BASE}/admin/operators/${userId}`, {
        method:'DELETE'
      });

      await loadOperators();
      setHint('operators_hint', `Оператор «${name}» удален.`, 'ok');
    }catch(e){
      setHint(
        'operators_hint',
        `Ошибка удаления: ${e?.message || String(e)}`,
        'error'
      );
    }
  };

  let exportDrivesCache = [];

  function renderExportDriveInfo(){
    const select = document.getElementById('exportDrive');
    const info = document.getElementById('exportDriveInfo');
    const button = document.getElementById('exportStartBtn');
    const unmountButton = document.getElementById('exportUnmountBtn');
    if(!select || !info || !button || !unmountButton) return;

    const drive = exportDrivesCache.find(x => String(x?.id || '') === select.value);
    if(!drive){
      info.textContent = 'Выберите доступный USB-носитель.';
      button.disabled = true;
      unmountButton.disabled = true;
      return;
    }

    const label = String(drive?.label || '').trim();
    const device = String(drive?.device || '').trim();
    const mount = String(drive?.mount_path || '').trim();
    const free = String(drive?.free_human || '—');
    const fs = String(drive?.filesystem || '').trim();
    const parts = [];
    if(label) parts.push(`Метка: ${label}`);
    if(device) parts.push(`Устройство: ${device}`);
    if(fs) parts.push(`ФС: ${fs}`);
    if(mount) parts.push(`Точка монтирования: ${mount}`);
    parts.push(`Свободно: ${free}`);
    if(drive?.writable === false){
      const roReason = String(drive?.read_only_reason || '');
      if(roReason === 'filesystem'){
        parts.push('ФС смонтирована только для чтения (возможна ошибка файловой системы)');
      }else if(roReason === 'device'){
        parts.push('устройство только для чтения');
      }else if(roReason === 'permissions'){
        parts.push('нет прав на запись');
      }else{
        parts.push('только чтение');
      }
    }
    info.textContent = parts.join(' · ');
    button.disabled = drive?.writable !== true;
    unmountButton.disabled = drive?.can_unmount !== true;
  }

  window.loadExportDrives = async function(){
    const select = document.getElementById('exportDrive');
    const button = document.getElementById('exportStartBtn');
    if(!select || !button) return;

    button.disabled = true;
    select.disabled = true;
    select.innerHTML = '<option value="">Поиск носителей…</option>';
    setHint('export_hint', 'Поиск USB-носителей…', 'waiting');

    try{
      const response = await fetchJson(`${API_BASE}/admin/export/drives`);
      exportDrivesCache = Array.isArray(response?.drives) ? response.drives : [];
      select.innerHTML = '';

      if(exportDrivesCache.length === 0){
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'USB-носители не найдены';
        select.appendChild(option);
        setHint(
          'export_hint',
          'Подключите и смонтируйте USB-носитель, затем нажмите «Обновить».',
          'error'
        );
      }else{
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = 'Выберите USB-носитель';
        select.appendChild(placeholder);

        for(const drive of exportDrivesCache){
          const option = document.createElement('option');
          option.value = String(drive?.id || '');
          const label = String(drive?.label || '').trim();
          const device = String(drive?.device || '').trim();
          const mount = String(drive?.mount_path || '').trim();
          const free = String(drive?.free_human || '—');
          const readonly = drive?.writable === true ? '' : ' [только чтение]';
          option.textContent = `${label ? label + ' · ' : ''}${device || mount} · свободно ${free}${readonly}`;
          select.appendChild(option);
        }
        setHint('export_hint', 'Выберите носитель для экспорта.', 'ok');
      }
    }catch(e){
      exportDrivesCache = [];
      select.innerHTML = '<option value="">Ошибка поиска носителей</option>';
      setHint(
        'export_hint',
        `Ошибка поиска USB: ${e?.message || String(e)}`,
        'error'
      );
    }finally{
      select.disabled = false;
      renderExportDriveInfo();
    }
  };

  window.startUsbExport = async function(){
    const select = document.getElementById('exportDrive');
    const button = document.getElementById('exportStartBtn');
    const driveId = String(select?.value || '');
    if(!driveId){
      setHint('export_hint', 'Выберите USB-носитель.', 'error');
      return;
    }

    const drive = exportDrivesCache.find(x => String(x?.id || '') === driveId);
    if(!drive || drive?.writable !== true){
      setHint('export_hint', 'Выбранный носитель недоступен для записи.', 'error');
      return;
    }

    if(button) button.disabled = true;
    if(select) select.disabled = true;
    setHint('export_hint', 'Формирование и запись экспорта…', 'waiting');

    try{
      const result = await fetchJson(`${API_BASE}/admin/export`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({drive_id:driveId})
      });
      setHint(
        'export_hint',
        `Экспорт завершен и данные синхронизированы с USB: ${result?.directory || 'каталог создан'}; ` +
        `партий: ${result?.batches_count ?? 0}, событий: ${result?.events_count ?? 0}, ` +
        `размер: ${result?.bytes_human || '—'}. ` +
        `Теперь нажмите «Безопасно извлечь» и дождитесь подтверждения размонтирования.`,
        'ok'
      );
    }catch(e){
      setHint(
        'export_hint',
        `Ошибка экспорта: ${e?.message || String(e)}`,
        'error'
      );
      await loadExportDrives();
    }finally{
      if(select) select.disabled = false;
      renderExportDriveInfo();
    }
  };

  window.safeUnmountUsb = async function(){
    const select = document.getElementById('exportDrive');
    const exportButton = document.getElementById('exportStartBtn');
    const unmountButton = document.getElementById('exportUnmountBtn');
    const driveId = String(select?.value || '');
    if(!driveId){
      setHint('export_hint', 'Выберите USB-носитель.', 'error');
      return;
    }

    const drive = exportDrivesCache.find(x => String(x?.id || '') === driveId);
    if(!drive || drive?.can_unmount !== true){
      setHint('export_hint', 'Для выбранного носителя автоматическое безопасное извлечение недоступно.', 'error');
      return;
    }

    if(exportButton) exportButton.disabled = true;
    if(unmountButton) unmountButton.disabled = true;
    if(select) select.disabled = true;
    setHint('export_hint', 'Синхронизация и безопасное размонтирование USB…', 'waiting');

    try{
      await fetchJson(`${API_BASE}/admin/export/unmount`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({drive_id:driveId})
      });
      setHint(
        'export_hint',
        'USB-носитель размонтирован. Теперь его можно физически извлечь.',
        'ok'
      );
      exportDrivesCache = [];
      if(select){
        select.innerHTML = '<option value=\"\">USB размонтирован — можно извлекать</option>';
      }
    }catch(e){
      setHint(
        'export_hint',
        `Ошибка безопасного извлечения: ${e?.message || String(e)}`,
        'error'
      );
      await loadExportDrives();
    }finally{
      if(select) select.disabled = false;
      renderExportDriveInfo();
    }
  };

  document.addEventListener('change', event => {
    if(event?.target?.id === 'exportDrive') renderExportDriveInfo();
  });

  function setStatusField(id, value, title=''){
    const el = document.getElementById(id);
    if(!el) return;

    el.classList.remove('statusOk','statusBad','statusUnknown');

    if(value === true){
      el.classList.add('statusOk');
    }else if(value === false){
      el.classList.add('statusBad');
    }else{
      el.classList.add('statusUnknown');
    }

    el.title = title || '';
  }

  function fmt1(value){
    const numberValue = Number(value);
    return Number.isFinite(numberValue)
      ? numberValue.toFixed(1)
      : '-';
  }

  function renderTemperatureField(fieldId, valueId, unitId, value, status, title=''){
    const field = document.getElementById(fieldId);
    const valueEl = document.getElementById(valueId);
    const unitEl = document.getElementById(unitId);

    if(!field || !valueEl || !unitEl) return;

    field.classList.remove('tempOk','tempNear','tempBad','tempUnknown');

    const normalizedStatus = String(status || 'unknown');

    if(normalizedStatus === 'ok'){
      field.classList.add('tempOk');
    }else if(normalizedStatus === 'near_critical'){
      field.classList.add('tempNear');
    }else if(
      normalizedStatus === 'out_of_range' ||
      normalizedStatus === 'missing'
    ){
      field.classList.add('tempBad');
    }else{
      field.classList.add('tempUnknown');
    }

    const numberValue = Number(value);
    const hasValue = (
      value !== null &&
      value !== undefined &&
      Number.isFinite(numberValue)
    );

    valueEl.textContent = hasValue ? numberValue.toFixed(1) : '-';
    unitEl.textContent = hasValue ? '°C' : '';
    field.title = title || '';
  }

  function renderHeader(){
    const st = stateCache;
    const settings = runtimeSettingsCache;

    setStatusField(
      'safety',
      st ? !!st.safety_ok : null,
      st
        ? (st.safety_ok ? 'Аварийный контур в норме' : 'Аварийный контур нарушен')
        : ''
    );

    setStatusField(
      'air_pressure',
      st ? st.air_pressure_ok !== false : null,
      st
        ? (st.air_pressure_ok !== false ? 'Давление воздуха в норме' : 'Нет давления воздуха')
        : ''
    );

    const rejectBinReplacementActive = (
      String(st?.mode || '') === 'paused_rejectbin'
    );

    setStatusField(
      'trash',
      st ? !!st.trash_present : null,
      st
        ? (
            rejectBinReplacementActive
              ? (st.trash_present ? 'Тара находится на датчике' : 'Тара снята с датчика')
              : (st.trash_present ? 'Тара брака установлена' : 'Тара брака отсутствует')
          )
        : ''
    );

    const tareEl = document.getElementById('reject_tare_no');
    const tareNo = Number(settings?.reject_bin_tare_no);

    if(tareEl){
      tareEl.textContent = (
        Number.isInteger(tareNo) &&
        tareNo >= 1 &&
        tareNo <= 10
      ) ? String(tareNo) : '-';
    }

    const rejectEl = document.getElementById('reject');

    if(rejectEl){
      rejectEl.classList.remove(
        'rejectNormal','rejectNear','rejectFull','rejectUnknown'
      );

      if(!st){
        rejectEl.textContent = '-';
        rejectEl.classList.add('rejectUnknown');
      }else{
        const count = Math.max(0, Number(st.reject_bin_count || 0));
        const capacity = Math.max(0, Number(st.reject_bin_capacity || 0));
        const near = Math.max(0, Math.floor(Number(settings?.rjb_near_full || 0)));

        rejectEl.textContent = capacity > 0
          ? `${count}/${capacity}`
          : `${count}/-`;

        if(capacity <= 0){
          rejectEl.classList.add('rejectUnknown');
        }else if(count >= capacity){
          rejectEl.classList.add('rejectFull');
        }else if(near > 0 && count >= Math.max(0, capacity - near)){
          rejectEl.classList.add('rejectNear');
        }else{
          rejectEl.classList.add('rejectNormal');
        }
      }
    }

    renderTemperatureField(
      'temp_loading_field',
      'temp_loading',
      'temp_loading_unit',
      settings?.temperature_sensor_loading_value,
      settings?.temperature_sensor_loading_status,
      (
        `Постамат: ${fmt1(settings?.temperature_sensor_loading_value)} °C; ` +
        `допуск ${fmt1(settings?.temperature_sensor_loading_min)}..` +
        `${fmt1(settings?.temperature_sensor_loading_max)} °C`
      )
    );

    renderTemperatureField(
      'temp_im_field',
      'temp_im',
      'temp_im_unit',
      settings?.temperature_sensor_im_value,
      settings?.temperature_sensor_im_status,
      (
        `Измерительная машина: ${fmt1(settings?.temperature_sensor_im_value)} °C; ` +
        `допуск ${fmt1(settings?.temperature_sensor_im_min)}..` +
        `${fmt1(settings?.temperature_sensor_im_max)} °C`
      )
    );

    renderAdminEmergencySoundButton();
  }

  function renderAdminEmergencySoundButton(){
    const button = document.getElementById('adminEmergencySoundBtn');
    if(!button) return;

    const emergencyActive = !!runtimeSettingsCache?.emergency_active;
    const muted = !!runtimeSettingsCache?.emergency_sound_muted;

    button.disabled = (
      !emergencyActive ||
      adminEmergencySoundToggleInFlight
    );
    button.classList.toggle('soundMuted', muted);
    button.setAttribute('aria-pressed', muted ? 'true' : 'false');
    button.textContent = muted ? '🔇' : '🔊';

    const actionText = muted
      ? 'Включить аварийный звук'
      : 'Отключить аварийный звук';

    button.setAttribute('aria-label', actionText);
    button.title = emergencyActive
      ? actionText
      : 'Аварийный звук сейчас не активен';
  }

  window.toggleAdminEmergencySound = async function(){
    if(adminEmergencySoundToggleInFlight) return;

    const emergencyActive = !!runtimeSettingsCache?.emergency_active;
    if(!emergencyActive) return;

    const previousMuted = !!runtimeSettingsCache?.emergency_sound_muted;
    const nextMuted = !previousMuted;

    adminEmergencySoundToggleInFlight = true;
    runtimeSettingsCache = {
      ...(runtimeSettingsCache || {}),
      emergency_sound_muted: nextMuted
    };
    renderAdminEmergencySoundButton();

    try{
      const command = await fetchJson(`${API_BASE}/commands`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          type:'SET_EMERGENCY_SOUND_MUTED',
          payload:{muted:nextMuted}
        })
      });

      const deadline = Date.now() + 10000;
      let commandCompleted = false;
      while(Date.now() < deadline){
        const current = await fetchJson(
          `${API_BASE}/commands/${Number(command?.id)}`
        );
        const status = String(current?.status || '').toLowerCase();

        if([
          'pending','processing','running','claimed','in_progress'
        ].includes(status)){
          await new Promise(resolve => setTimeout(resolve, 200));
          continue;
        }

        if(current?.error || status === 'failed' || status === 'error'){
          throw new Error(current?.error || 'Команда завершилась с ошибкой');
        }
        commandCompleted = true;
        break;
      }

      if(!commandCompleted){
        throw new Error('Не получен результат выполнения команды');
      }

      await refreshRuntimeSettings();
    }catch(e){
      runtimeSettingsCache = {
        ...(runtimeSettingsCache || {}),
        emergency_sound_muted: previousMuted
      };
      renderHeader();
      window.alert(
        `Не удалось изменить аварийный звук: ${e?.message || String(e)}`
      );
    }finally{
      adminEmergencySoundToggleInFlight = false;
      renderAdminEmergencySoundButton();
    }
  };

  function setHint(id, message, kind=''){
    const el = document.getElementById(id);
    if(!el) return;

    el.className = `hint ${kind}`.trim();
    el.textContent = message || '';
  }

  async function fetchJson(url, options){
    const response = await fetch(url, options);
    if(response.status === 401){
      location.replace(`${API_BASE}/login`);
      throw new Error('Требуется авторизация.');
    }
    const text = await response.text();

    let body;
    try{
      body = text ? JSON.parse(text) : null;
    }catch(e){
      body = text;
    }

    if(!response.ok){
      const message = (
        body && typeof body.detail === 'string'
          ? body.detail
          : typeof body === 'string' && body.trim()
          ? body.trim()
          : `HTTP ${response.status}`
      );
      throw new Error(message);
    }

    return body;
  }

  async function refreshRuntimeSettings(){
    try{
      runtimeSettingsCache = await fetchJson(`${API_BASE}/settings`);
      renderHeader();
    }catch(e){
      // Оставляем последнее успешно полученное состояние статусной строки.
    }
  }

  window.loadSettings = async function(){
    setHint('set_hint', 'Загрузка…', 'waiting');

    try{
      const settings = await fetchJson(`${API_BASE}/settings`);
      runtimeSettingsCache = settings;
      window._settingsCache = settings;

      document.getElementById('set_ppod').value =
        settings.settings_ppod ?? '';
      document.getElementById('set_consecutive_rejects_threshold').value =
        settings.consecutive_rejects_threshold ?? '';
      document.getElementById('set_reject_cap').value =
        settings.reject_bin_capacity ?? '';
      document.getElementById('set_rjb_near_full').value =
        settings.rjb_near_full ?? '';
      document.getElementById('set_t1_min').value =
        settings.temperature_sensor_loading_min ?? '';
      document.getElementById('set_t1_max').value =
        settings.temperature_sensor_loading_max ?? '';
      document.getElementById('set_t2_min').value =
        settings.temperature_sensor_im_min ?? '';
      document.getElementById('set_t2_max').value =
        settings.temperature_sensor_im_max ?? '';
      document.getElementById('set_temp_near_critical').value =
        settings.temp_near_critical ?? '';
      document.getElementById('set_layout').value =
        String(settings.layout ?? 2);
      document.getElementById('set_use_alternate_wave').checked =
        !!settings.use_alternate_wave;
      document.getElementById('set_udp_server_ip').value =
        settings.udp_server_ip ?? '';
      document.getElementById('set_udp_server_port').value =
        settings.udp_server_port ?? '';

      renderHeader();
      setHint('set_hint', 'Настройки загружены.', 'ok');
    }catch(e){
      setHint(
        'set_hint',
        `Ошибка загрузки: ${e?.message || String(e)}`,
        'error'
      );
    }
  };

  window.saveSettings = async function(){
    const patch = {};
    const cache = window._settingsCache || {};

    function readNumber(id, field, options={}){
      const raw = val(id);
      if(raw === '') return null;

      const numberValue = Number(raw);
      if(!Number.isFinite(numberValue)){
        throw new Error(`${field}: укажите число`);
      }

      if(options.min !== undefined && numberValue < options.min){
        throw new Error(`${field}: значение не может быть меньше ${options.min}`);
      }

      return options.integer ? Math.floor(numberValue) : numberValue;
    }

    try{
      const ppod = readNumber(
        'set_ppod',
        'Допустимая доля брака',
        {min:0}
      );

      if(ppod !== null && ppod > 1){
        throw new Error(
          'Допустимая доля брака должна находиться в диапазоне от 0 до 1'
        );
      }

      const consecutiveRejectsThreshold = readNumber(
        'set_consecutive_rejects_threshold',
        'Количество браков подряд до проверки',
        {min:1}
      );

      if(
        consecutiveRejectsThreshold !== null &&
        !Number.isInteger(consecutiveRejectsThreshold)
      ){
        throw new Error(
          'Количество браков подряд до проверки должно быть целым числом больше 0'
        );
      }

      const capacity = readNumber(
        'set_reject_cap',
        'Кол-во ячеек в таре брака',
        {min:0, integer:true}
      );
      const nearFull = readNumber(
        'set_rjb_near_full',
        'Предупреждать за N ячеек',
        {min:0, integer:true}
      );
      const loadingMin = readNumber(
        'set_t1_min',
        'Мин. температура на постаматах'
      );
      const loadingMax = readNumber(
        'set_t1_max',
        'Макс. температура на постаматах'
      );
      const imMin = readNumber(
        'set_t2_min',
        'Мин. температура на микрометре'
      );
      const imMax = readNumber(
        'set_t2_max',
        'Макс. температура на микрометре'
      );
      const nearCritical = readNumber(
        'set_temp_near_critical',
        'Предупреждение за N °C',
        {min:0}
      );

      const layout = readNumber(
        'set_layout',
        'Раскладка',
        {min:0, integer:true}
      );

      if(layout === null || layout > 3){
        throw new Error(
          'Раскладка должна быть выбрана из диапазона А..Г'
        );
      }

      const udpServerIp = val('set_udp_server_ip');
      const udpServerPortRaw = val('set_udp_server_port');

      if(
        (udpServerIp === '') !==
        (udpServerPortRaw === '')
      ){
        throw new Error(
          'IP-адрес и порт UDP-сервера должны быть заполнены вместе'
        );
      }

      let udpServerPort = null;
      if(udpServerPortRaw !== ''){
        udpServerPort = Number(udpServerPortRaw);

        if(
          !Number.isInteger(udpServerPort) ||
          udpServerPort < 1 ||
          udpServerPort > 65535
        ){
          throw new Error(
            'Порт UDP-сервера должен быть целым числом от 1 до 65535'
          );
        }
      }

      if(ppod !== null) patch.settings_ppod = ppod;
      if(consecutiveRejectsThreshold !== null){
        patch.consecutive_rejects_threshold = consecutiveRejectsThreshold;
      }
      if(capacity !== null) patch.reject_bin_capacity = capacity;
      if(nearFull !== null) patch.rjb_near_full = nearFull;
      if(loadingMin !== null) patch.temperature_sensor_loading_min = loadingMin;
      if(loadingMax !== null) patch.temperature_sensor_loading_max = loadingMax;
      if(imMin !== null) patch.temperature_sensor_im_min = imMin;
      if(imMax !== null) patch.temperature_sensor_im_max = imMax;
      if(nearCritical !== null) patch.temp_near_critical = nearCritical;

      patch.layout = layout;

      patch.use_alternate_wave =
        !!document.getElementById('set_use_alternate_wave')?.checked;

      if(udpServerIp !== '' && udpServerPort !== null){
        patch.udp_server_ip = udpServerIp;
        patch.udp_server_port = udpServerPort;
      }

      const effectiveLoadingMin = (
        patch.temperature_sensor_loading_min ??
        cache.temperature_sensor_loading_min
      );
      const effectiveLoadingMax = (
        patch.temperature_sensor_loading_max ??
        cache.temperature_sensor_loading_max
      );
      const effectiveImMin = (
        patch.temperature_sensor_im_min ??
        cache.temperature_sensor_im_min
      );
      const effectiveImMax = (
        patch.temperature_sensor_im_max ??
        cache.temperature_sensor_im_max
      );

      if(
        Number.isFinite(Number(effectiveLoadingMin)) &&
        Number.isFinite(Number(effectiveLoadingMax)) &&
        Number(effectiveLoadingMin) >= Number(effectiveLoadingMax)
      ){
        throw new Error(
          'Минимальная температура на постаматах должна быть меньше максимальной'
        );
      }

      if(
        Number.isFinite(Number(effectiveImMin)) &&
        Number.isFinite(Number(effectiveImMax)) &&
        Number(effectiveImMin) >= Number(effectiveImMax)
      ){
        throw new Error(
          'Минимальная температура на микрометре должна быть меньше максимальной'
        );
      }

      setHint('set_hint', 'Сохранение…', 'waiting');

      await fetchJson(`${API_BASE}/settings`, {
        method:'PUT',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify(patch)
      });

      await window.loadSettings();
      setHint('set_hint', 'Настройки сохранены.', 'ok');
    }catch(e){
      setHint(
        'set_hint',
        `Ошибка сохранения: ${e?.message || String(e)}`,
        'error'
      );
    }
  };

  window.sendCmd = async function(type, payload){
    const hintId = type.startsWith('IM_') ? 'im_hint' : 'rtk_hint';
    setHint(hintId, `Отправка ${type}…`, 'waiting');

    try{
      const response = await fetchJson(`${API_BASE}/commands`, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          type,
          payload:payload ?? {}
        })
      });

      setHint(
        hintId,
        `Команда ${type} принята, id=${response?.id ?? '-'}.`,
        'ok'
      );
    }catch(e){
      setHint(
        hintId,
        `Ошибка ${type}: ${e?.message || String(e)}`,
        'error'
      );
    }
  };

  async function pollState(){
    try{
      stateCache = await fetchJson(`${API_BASE}/state`);
      renderHeader();
    }catch(e){
      // Оставляем последнее успешно полученное состояние статусной строки.
    }
  }

  renderCurrentDateTime();
  loadCurrentUser();
  window.loadSettings();
  window.loadOperators();
  window.loadExportDrives();
  pollState();

  setInterval(renderCurrentDateTime, 1000);
  setInterval(pollState, 500);
  setInterval(refreshRuntimeSettings, 1000);
</script>
</body>
</html>
"""
    return HTMLResponse(html)
