import time
import json
import logging
import math 
import re
import os
import textwrap
from datetime import datetime, timezone

from typing import Callable, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import select

from app.common.enums import CommandType, EventType, Severity, SystemMode, CommandStatus
from app.common.timeutils import utcnow
from app.common.runtime_paths import get_temperature_snapshot_path
from app.common.product_rules import (
    ProductRuleError,
    product_code_from_name_and_spec,
    resolve_product_rule,
    uses_special_unloading_cell,
)
from app.daemon.io_base import IOBase
from app.daemon.rtk_port import RTKPort
from app.daemon.rtk_http import HttpRTK
from app.daemon.printer import PrinterBackend, CupsPrinter#FilePrinter
from app.daemon.measurement_parse import parse_measurement_payload
from app.daemon.utils import parse_product_name_and_spec, build_measurement_items
from app.daemon.udp_client import send_batch_data
from app.infra import repo
from app.infra.models import CommandRow, BatchRow, RejectBinItemRow, RejectBinRow

SPECIAL_UNLOADING_CELL = 16

REJECT_BIN_TARE_MIN = 1
REJECT_BIN_TARE_MAX = 10
REJECT_BIN_REPLACEMENT_TARE_SETTING = "rejectbin_replacement_tare_no"
REJECT_BIN_REPLACEMENT_OPERATOR_SETTING = "rejectbin_replacement_operator_name"
REJECT_BIN_WORKFLOW_MESSAGE_KEY = "reject_bin.workflow"
REJECT_BIN_RESET_COUNTER_PENDING_SETTING = (
    "rejectbin_reset_defect_counter_pending"
)

CALIBRATION_RETRY_SAFETY_BATCH_SETTING = (
    "calibration_retry_safety_batch_id"
)
CALIBRATION_RETRY_SAFETY_OPERATION_SETTING = (
    "calibration_retry_safety_operation_id"
)
CHECK_TO_CALIBRATION_MARKER = "auto_after_failed_check=1"

QUALITY_PPOD_MESSAGE_PREFIX = "quality.ppod"
QUALITY_CALIBRATION_MESSAGE_PREFIX = "quality.calibration"
STOP_MESSAGE_PREFIX = "stop"
IM_WORKFLOW_MESSAGE_PREFIX = "im.workflow"
BATCH_RESULT_MESSAGE_PREFIX = "batch.result"
BATCH_START_MESSAGE_PREFIX = "batch.start"

PAUSE_RECOVERY_MESSAGE_KEYS = (
    "safety.recovered",
    "trash.recovered",
    "temperature.recovered",
    "air.recovered",
)

TRANSIENT_INFO_MESSAGE_TTL_SEC = 8.0
TRANSIENT_WARN_MESSAGE_TTL_SEC = 12.0

EMERGENCY_ACTIVE_SETTING = "emergency_active"
EMERGENCY_SOUND_MUTED_SETTING = "emergency_sound_muted"

STOP_CLEANUP_PHASE = "stopping_after_stop"
STOP_AFTER_BATCH_SETTING = "stop_after_batch_id"

DEFAULT_CONSECUTIVE_REJECTS_THRESHOLD = 3
DEFAULT_BATCH_LAYOUT = 2
DEFAULT_USE_ALTERNATE_WAVE = True

try:
    CHECK_TO_CALIBRATION_REARM_SEC = max(
        1.0,
        float(
            os.getenv(
                "CHECK_TO_CALIBRATION_REARM_SEC",
                "10.0",
            )
        ),
    )
except (TypeError, ValueError):
    CHECK_TO_CALIBRATION_REARM_SEC = 10.0

TEMPERATURE_SNAPSHOT_FILE = get_temperature_snapshot_path()

TRY_TEMP_SNAPSHOT_PERIOD = os.getenv(
    "TEMPERATURE_SNAPSHOT_PERIOD_SEC",
    "1.0",
)
try:
    TEMPERATURE_SNAPSHOT_PERIOD_SEC = max(
        0.2,
        float(TRY_TEMP_SNAPSHOT_PERIOD),
    )
except (TypeError, ValueError):
    TEMPERATURE_SNAPSHOT_PERIOD_SEC = 1.0


log = logging.getLogger(__name__)


def _batch_operator_message_key(
    prefix: str,
    batch_id,
) -> str | None:
    """Возвращает batch-scoped ключ сообщения либо None вне партии."""
    try:
        bid = int(batch_id)
    except (TypeError, ValueError):
        return None

    if bid <= 0:
        return None

    return f"{str(prefix)}.{bid}"


def _clear_batch_operator_messages(
    db: Session,
    batch_id,
    *prefixes: str,
) -> None:
    """Удаляет только перечисленные сообщения указанной партии."""
    for prefix in prefixes:
        key = _batch_operator_message_key(prefix, batch_id)
        if key:
            repo.clear_operator_message(db, key)


def _clear_completed_batch_operator_messages(
    db: Session,
    *,
    keep_batch_id=None,
) -> None:
    """
    Снимает итоговые сообщения ранее завершённых партий.

    Вызывается только при переходе системы в idle либо непосредственно
    перед началом разбора следующей партии. Состояния партий, команды РТК
    и производственные latch-флаги функция не изменяет.
    """
    prefixes = (
        QUALITY_PPOD_MESSAGE_PREFIX,
        QUALITY_CALIBRATION_MESSAGE_PREFIX,
        STOP_MESSAGE_PREFIX,
        IM_WORKFLOW_MESSAGE_PREFIX,
        BATCH_RESULT_MESSAGE_PREFIX,
        BATCH_START_MESSAGE_PREFIX,
    )

    keep_keys: set[str] = set()
    for prefix in prefixes:
        keep_key = _batch_operator_message_key(
            prefix,
            keep_batch_id,
        )
        if keep_key:
            keep_keys.add(keep_key)

    for item in repo.list_operator_messages(db):
        key = str(item.get("key") or "")

        if key in keep_keys:
            continue

        if any(
            key.startswith(f"{prefix}.")
            for prefix in prefixes
        ):
            repo.clear_operator_message(db, key)


def _normalize_rejectbin_tare_no(value) -> int | None:
    """
    Возвращает номер физической тары 1..10
    либо None для некорректного значения.
    """
    if isinstance(value, bool):
        return None

    try:
        tare_no = int(value)
    except (TypeError, ValueError):
        return None

    if not REJECT_BIN_TARE_MIN <= tare_no <= REJECT_BIN_TARE_MAX:
        return None

    return tare_no


def _dt_now_local_str() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def _parse_iso_dt(s: str) -> datetime | None:
    if not s:
        return None
    s = str(s).strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # если naive — считаем что это UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _fmt_report_dt(value) -> str:
    """
    Форматирование дат для печатных протоколов.

    В БД время обычно хранится как UTC-naive через utcnow().
    Поэтому naive datetime считаем UTC и переводим в локальное время ОС.
    """
    if not value:
        return "-"

    if isinstance(value, datetime):
        dt = value
    else:
        dt = _parse_iso_dt(str(value))
        if not dt:
            return str(value)

    try:
        # Важный момент:
        # даты из БД приходят без tzinfo, но фактически это UTC.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        # Переводим в локальный часовой пояс ОС.
        dt = dt.astimezone()
    except Exception:
        pass

    return dt.strftime("%d.%m.%Y %H:%M")


def _fmt_duration_ru(start_iso: str | None, end_dt_utc: datetime | None) -> str:
    st = _parse_iso_dt(start_iso or "")
    if not st:
        return "-"
    end = end_dt_utc
    if end is None:
        end = datetime.now(timezone.utc)
    elif end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    sec = max(0, int((end - st).total_seconds()))
    total_min = sec // 60
    h = total_min // 60
    m = total_min % 60
    return f"{h}ч {m}мин."


def _fmt_cells(batch: BatchRow) -> str:
    d = batch.data or {}
    loc = batch.location or {}

    cells = d.get("out_tare_ids") or loc.get("out_tare_ids") or []
    if not isinstance(cells, list) or not cells:
        cn = loc.get("cell_no")
        cells = [cn] if cn is not None else []

    xs = []
    for x in cells:
        try:
            xs.append(int(x))
        except Exception:
            pass

    if not xs:
        return "-"

    xs = sorted(set(xs))
    return ", ".join([f"{x:02d}" if 0 <= x < 100 else str(x) for x in xs])


def _batch_side_cell_ids(
    batch: BatchRow,
    *,
    side: str,
) -> list[int]:
    """
    Возвращает нормализованные номера ячеек партии
    для указанной стороны.

    location.cell_no исторически относится только
    к загрузочной ячейке.

    Для специальной детали выгрузочная ячейка всегда №16.
    """
    data = batch.data or {}
    location = batch.location or {}

    product_code = str(
        data.get("product_code") or ""
    ).strip()

    if (
        side == "unloading"
        and uses_special_unloading_cell(product_code)
    ):
        return [int(SPECIAL_UNLOADING_CELL)]

    if side == "loading":
        key = "in_tare_ids"
    elif side == "unloading":
        key = "out_tare_ids"
    else:
        raise ValueError(
            f"unknown batch side: {side}"
        )

    raw = (
        data.get(key)
        or location.get(key)
        or []
    )

    if not raw:
        fallback = (
            location.get("cell_no")
            or data.get("cell_no")
        )

        raw = (
            []
            if fallback is None
            else [fallback]
        )

    if not isinstance(raw, list):
        raw = [raw]

    result: list[int] = []

    for value in raw:
        try:
            cell_no = int(value)
        except (TypeError, ValueError):
            continue

        if (
            cell_no > 0
            and cell_no not in result
        ):
            result.append(cell_no)

    return result


def _batch_door_cells(
    batch: BatchRow,
) -> tuple[int | None, int | None]:
    in_ids = _batch_side_cell_ids(
        batch,
        side="loading",
    )
    out_ids = _batch_side_cell_ids(
        batch,
        side="unloading",
    )

    loading_cell = (
        int(in_ids[0])
        if in_ids
        else None
    )
    unloading_cell = (
        int(out_ids[0])
        if out_ids
        else None
    )

    return loading_cell, unloading_cell


def _batch_doors_wait_message(
    *,
    batch_id: int,
    loading_cell: int,
    unloading_cell: int,
    loading_closed,
    unloading_closed,
) -> str:
    loading_open = loading_closed is not True
    unloading_open = unloading_closed is not True

    if loading_open and unloading_open:
        return (
            f"Партия не запущена: "
            "закройте двери ячеек "
            f"загрузки №{int(loading_cell)} "
            f"и выгрузки №{int(unloading_cell)}"
        )

    if loading_open:
        return (
            f"Партия не запущена: "
            "закройте дверь ячейки "
            f"загрузки №{int(loading_cell)}"
        )

    return (
        f"Партия не запущена: "
        "закройте дверь ячейки "
        f"выгрузки №{int(unloading_cell)}"
    )


def _batch_door_seen_flags(
    batch: BatchRow,
) -> tuple[bool, bool]:
    """
    Возвращает признаки того, что двери партии действительно
    хотя бы один раз были замечены открытыми после создания.

    Для старых партий, созданных до появления этих полей,
    используем True как совместимый fallback: такие партии
    по-прежнему могут перейти в loaded по закрытым дверям.
    """
    data = batch.data or {}

    loading_seen = (
        bool(data.get("loading_door_seen_open"))
        if "loading_door_seen_open" in data
        else True
    )
    unloading_seen = (
        bool(data.get("unloading_door_seen_open"))
        if "unloading_door_seen_open" in data
        else True
    )

    return loading_seen, unloading_seen


def _batch_doors_open_wait_message(
    *,
    batch_id: int,
    loading_cell: int,
    unloading_cell: int,
    loading_seen_open: bool,
    unloading_seen_open: bool,
) -> str:
    loading_wait = not bool(loading_seen_open)
    unloading_wait = not bool(unloading_seen_open)

    if loading_wait and unloading_wait:
        return (
            f"Партия ожидает загрузки: "
            "ожидание открытия дверей ячеек "
            f"загрузки №{int(loading_cell)} "
            f"и выгрузки №{int(unloading_cell)}"
        )

    if loading_wait:
        return (
            f"Партия ожидает загрузки: "
            "ожидание открытия двери ячейки "
            f"загрузки №{int(loading_cell)}"
        )

    return (
        f"Партия ожидает загрузки: "
        "ожидание открытия двери ячейки "
        f"выгрузки №{int(unloading_cell)}"
    )



def _batch_doors_message_key(batch_id: int) -> str:
    return f"batch.{int(batch_id)}.doors"

def _first_out_cell(batch: BatchRow) -> int:
    data = batch.data or {}
    location = batch.location or {}

    raw = data.get("out_tare_ids") or location.get("out_tare_ids") or []

    if not isinstance(raw, list):
        raw = [raw]

    for value in raw:
        try:
            cell_no = int(value)
        except (TypeError, ValueError):
            continue

        if cell_no > 0:
            return cell_no

    # Fallback только для старых партий.
    fallback = location.get("cell_no") or data.get("cell_no")
    try:
        return int(fallback or 0)
    except (TypeError, ValueError):
        return 0


def _norm_out_tares(batch: BatchRow) -> list[dict]:
    d = batch.data or {}
    xs = d.get("out_tares") or d.get("extracted_out_tares")
    if not isinstance(xs, list):
        return []
    out = []
    for it in xs:
        if not isinstance(it, dict):
            continue
        try:
            cell_no = int(it.get("cell_no"))
            tare_no = int(it.get("tare_no"))
            qty = int(it.get("qty"))
        except Exception:
            continue
        if cell_no > 0 and tare_no > 0:
            out.append({"cell_no": cell_no, "tare_no": tare_no, "qty": max(0, qty)})
    out.sort(key=lambda x: (x["cell_no"], x["tare_no"]))
    return out


def _upsert_out_tare(batch: BatchRow, *, cell_no: int, tare_no: int, qty: int) -> bool:
    """
    Записывает/обновляет раскладку годных деталей по тарам выгрузки.

    batch.data - JSON-поле. Нельзя мутировать вложенный список "на месте"
    без flag_modified(), иначе SQLAlchemy может не сохранить изменения.
    """
    d = dict(batch.data or {})

    raw = d.get("out_tares")
    if isinstance(raw, list):
        xs = [
            dict(it)
            for it in raw
            if isinstance(it, dict)
        ]
    else:
        xs = []

    cell_no_i = int(cell_no)
    tare_no_i = int(tare_no)
    qty_i = int(qty)

    for it in xs:
        try:
            same_cell = int(it.get("cell_no", -1)) == cell_no_i
            same_tare = int(it.get("tare_no", -1)) == tare_no_i
        except Exception:
            same_cell = False
            same_tare = False

        if same_cell and same_tare:
            it["cell_no"] = cell_no_i
            it["tare_no"] = tare_no_i
            it["qty"] = qty_i
            d["out_tares"] = xs
            batch.data = d
            flag_modified(batch, "data")
            return True

    xs.append({
        "cell_no": cell_no_i,
        "tare_no": tare_no_i,
        "qty": qty_i,
    })

    xs.sort(key=lambda x: (int(x.get("cell_no") or 0), int(x.get("tare_no") or 0)))

    d["out_tares"] = xs
    batch.data = d
    flag_modified(batch, "data")
    return True


def _fmt_cells_with_tares(batch: BatchRow) -> str:
    xs = _norm_out_tares(batch)
    if not xs:
        return "-"
    return ", ".join([f"{x['cell_no']:02d}-{x['tare_no']}" for x in xs])

def _get_counts(batch: BatchRow) -> tuple[int, int]:
    d = batch.data or {}
    mg = getattr(batch, "measured_good", None)
    mb = getattr(batch, "measured_bad", None)

    if mg is None:
        mg = d.get("measured_good")
    if mb is None:
        mb = d.get("measured_bad")

    # fallback на старые ключи
    if mg is None:
        mg = d.get("ok_qty", 0)
    if mb is None:
        mb = d.get("nok_qty", 0)

    return int(mg or 0), int(mb or 0)


def _fmt_meas_value(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, (int, float)):
        return f"{v:.4f}"
    s = str(v).strip()
    try:
        return f"{float(s):.4f}"
    except Exception:
        return s


def _measurement_items_all_zero(items: list) -> bool:
    """True, если непустой набор размеров содержит только числовые нули."""
    if not isinstance(items, list) or not items:
        return False

    for item in items:
        if not isinstance(item, dict) or "value" not in item:
            return False

        value = item.get("value")
        if value is None or isinstance(value, bool):
            return False

        try:
            number = float(str(value).strip().replace(",", "."))
        except (TypeError, ValueError):
            return False

        if not math.isfinite(number) or number != 0.0:
            return False

    return True


def _batch_document_prefix(batch: BatchRow) -> str:
    """Префикс имени печатного файла: номер маршрутного паспорта."""
    data = batch.data or {}
    passport_number = str(data.get("passport_number") or "").strip()
    return passport_number or str(int(batch.id))


def _build_defect_protocol_text(
    batch: BatchRow,
    reject_items_all: list[RejectBinItemRow],
    *,
    tare_no_by_bin_id: dict[int, int | None] | None = None,
    operator_name: str = "-",
) -> str | None:
    mg, mb = _get_counts(batch)
    if not reject_items_all:
        reject_items_all = []

    bid = int(batch.id)
    tare_no_by_bin_id = tare_no_by_bin_id or {}

    # сортировка строго по таре и порядку добавления
    items_sorted = sorted(
        reject_items_all,
        key=lambda it: (int(it.reject_bin_id or 0), int(it.id or 0)),
    )

    # сквозная нумерация ячеек ВНУТРИ ТАРЫ (а не внутри партии)
    cell_map: dict[int, int] = {}
    per_bin: dict[int, int] = {}
    for it in items_sorted:
        rb = int(it.reject_bin_id or 0)
        per_bin[rb] = per_bin.get(rb, 0) + 1
        cell_map[int(it.id)] = per_bin[rb]

    batch_bin_ids = {int(it.reject_bin_id or 0) for it in items_sorted if int(it.batch_id or 0) == bid}
    multi_bins = len(batch_bin_ids) > 1

    defect_lines: list[str] = []
    for it in items_sorted:
        if int(it.batch_id or 0) != bid:
            continue

        mp = it.measured_params or {}

        cell_no = cell_map.get(int(it.id), 0)
        reject_bin_id = int(it.reject_bin_id or 0)
        physical_tare_no = tare_no_by_bin_id.get(reject_bin_id)
        physical_tare_text = (
            str(int(physical_tare_no))
            if physical_tare_no is not None
            else "-"
        )
        prefix = (
            f"[{cell_no}]:"
            if not multi_bins
            else f"[{cell_no}] (тара №{physical_tare_text}):"
        )

        if isinstance(mp, dict) and bool(mp.get("unmeasured")):
            defect_lines.append(f"{prefix}  не измерено")
            continue

        meas_items = (
            mp.get("items")
            if isinstance(mp, dict)
            else None
        )

        if not isinstance(meas_items, list):
            continue

        if _measurement_items_all_zero(meas_items):
            defect_lines.append(f"{prefix}  не измерено")
            continue

        bad = [
            x
            for x in meas_items
            if isinstance(x, dict)
            and x.get("ok") is False
        ]

        if not bad:
            continue

        parts = []

        for x in bad:
            name = (
                x.get("name")
                or x.get("param")
                or x.get("key")
                or "-"
            )
            parts.append(
                f"{name}:{_fmt_meas_value(x.get('value'))}"
            )

        defect_lines.append(
            f"{prefix}    {', '.join(parts)}"
        )

    d = batch.data or {}
    passport_number = d.get("passport_number") or "-"
    product_name = d.get("product_name") or "-"
    product_code = d.get("product_code") or "-"
    rod_batch_number = d.get("rod_batch_number") or "-"

    if not defect_lines:
        defect_lines = ["Дефекты по партии не обнаружены."]

    total = mg + mb
    printed_at = _dt_now_local_str()

    return "\n".join([
        "Протокол брака партии",
        "",
        f"Маршрутный паспорт:           {passport_number}",
        f"Наименование детали:          {product_name}",
        f"Обозначение детали:           {product_code}",
        f"Номер партии прутка:          {rod_batch_number}",
        "",
        *defect_lines,
        "",
        f"Деталей проконтролировано:    {total} шт.",
        f"Принято годных:               {mg} шт.",
        f"С отклонением(брак):          {mb} шт.",
        "",
        f"Оператор:                     {operator_name}",
        f"Дата и время печати протокола:{printed_at}",
    ])


def _build_protocol_text(
    batch: BatchRow,
    *,
    operator_name: str = "-",
) -> str:
    d = batch.data or {}

    passport_number = d.get("passport_number") or "-"
    product_name = d.get("product_name") or "-"
    product_code = d.get("product_code") or "-"
    rod_batch_number = d.get("rod_batch_number") or "-"

    mg, mb = _get_counts(batch)
    total = mg + mb

    try:
        unmeasured_after_stop = int(
            d.get("unmeasured_after_stop_qty") or 0
        )
    except Exception:
        unmeasured_after_stop = 0

    duration = _fmt_duration_ru(d.get("started_ts"), batch.finished_at)
    cells = _fmt_cells_with_tares(batch)
    if cells == "-":
        # fallback, если по таре ещё не собрали
        cells = _fmt_cells(batch)

    printed_at = _dt_now_local_str()

    return "\n".join([
        "Протокол партии",
        "",
        f"Маршрутный паспорт:           {passport_number}",
        f"Наименование детали:          {product_name}",
        f"Обозначение детали:           {product_code}",
        f"Номер партии прутка:          {rod_batch_number}",
        "",
        f"Деталей проконтролировано:    {total} шт.",
        f"Принято годных:               {mg} шт.",
        f"С отклонением(брак):          {mb} шт.",
        *(
            [
                f"Не измерено после СТОП:       "
                f"{unmeasured_after_stop} шт."
            ]
            if unmeasured_after_stop > 0
            else []
        ),        
        "",
        f"Время обработки:              {duration}",
        "",
        f"Ячейки:                       {cells}",
        "",
        f"Оператор:                     {operator_name}",
        f"Дата и время печати протокола:{printed_at}",
    ])


def _build_label_text(batch: BatchRow, *, qty: int) -> str:
    d = batch.data or {}

    blank_alloy = d.get("blank_alloy") or "-"
    rod_batch_number = d.get("rod_batch_number") or "-"
    product_code = d.get("product_code") or "-"
    passport_number = d.get("passport_number") or "-"
    passport_date = d.get("passport_date") or "-"
    total = int(qty)

    return "\n".join([
        "                       ф.290-115",
        f"      Сплав {blank_alloy}",
        "",
        f"Партия {rod_batch_number}",
        "",
        f"Чертеж № {product_code}",
        "",
        f"МП № {passport_number} от {passport_date}",
        "",
        f"Количество {total} шт.",
        "",
    ])


def _build_labels_sheet_text(batch: BatchRow, tares: list[dict]) -> str:
    """
    Формирует один файл/лист ярлыков для одной ячейки выгрузки.

    На выходе:
    - ярлыки идут сверху вниз по возрастанию номера тары;
    - каждый ярлык отделён горизонтальной линией;
    - после последней тары тоже есть горизонтальная линия;
    - справа у всех строк есть вертикальная линия "|".
    """
    valid: list[dict] = []

    for t in tares:
        if not isinstance(t, dict):
            continue

        try:
            cell_no = int(t.get("cell_no") or 0)
            tare_no = int(t.get("tare_no") or 0)
            qty = int(t.get("qty") or 0)
        except Exception:
            continue

        if cell_no <= 0 or tare_no <= 0 or qty <= 0:
            continue

        valid.append({
            "cell_no": cell_no,
            "tare_no": tare_no,
            "qty": qty,
        })

    valid.sort(key=lambda x: int(x["tare_no"]))

    label_blocks: list[list[str]] = []

    for t in valid:
        cell_no = int(t["cell_no"])
        tare_no = int(t["tare_no"])

        block = _build_label_text(
            batch,
            qty=int(t["qty"]),
        ).splitlines()

        block.append(f"Паллета {cell_no:02d}-{tare_no}")

        label_blocks.append(block)

    if not label_blocks:
        return ""

    max_line_len = 0
    for block in label_blocks:
        for line in block:
            max_line_len = max(max_line_len, len(line))

    width = max(34, max_line_len)
    sep = "-" * width

    lines: list[str] = []

    for block in label_blocks:
        for line in block:
            lines.append(f"{line:<{width}}|")
        lines.append(f"{sep}|")

    return "\n".join(lines)


def _clip_cell(value, width: int) -> str:
    s = str(value if value is not None else "-").strip()
    if not s:
        s = "-"
    if len(s) <= width:
        return s
    if width <= 1:
        return s[:width]
    return s[: width - 1] + "…"


def _table_line(widths: list[int]) -> str:
    return "|" + "|".join("-" * (w + 2) for w in widths) + "|"


def _table_row(values: list, widths: list[int]) -> str:
    cells = []
    for value, width in zip(values, widths):
        s = _clip_cell(value, width)
        cells.append(f" {s:<{width}} ")
    return "|" + "|".join(cells) + "|"


def _wrap_table_cell(value, width: int) -> list[str]:
    s = str(value if value is not None else "-").strip() or "-"
    if width <= 0:
        return [""]

    wrapped = textwrap.wrap(
        s,
        width=width,
        break_long_words=True,
        break_on_hyphens=False,
        replace_whitespace=True,
        drop_whitespace=True,
    )
    return wrapped or ["-"]


def _table_wrapped_rows(values: list, widths: list[int]) -> list[str]:
    """Формирует строку таблицы в несколько физических строк без обрезки."""
    wrapped_cells = [
        _wrap_table_cell(value, width)
        for value, width in zip(values, widths)
    ]
    height = max((len(cell) for cell in wrapped_cells), default=1)

    rows: list[str] = []
    for line_index in range(height):
        cells: list[str] = []
        for wrapped, width in zip(wrapped_cells, widths):
            value = wrapped[line_index] if line_index < len(wrapped) else ""
            cells.append(f" {value:<{width}} ")
        rows.append("|" + "|".join(cells) + "|")

    return rows


def _fmt_int_ranges(values: list[int]) -> str:
    xs = sorted({int(x) for x in values})

    if not xs:
        return "-"

    ranges: list[str] = []
    start = prev = xs[0]

    for value in xs[1:]:
        if value == prev + 1:
            prev = value
            continue

        ranges.append(
            str(start)
            if start == prev
            else f"{start}-{prev}"
        )
        start = prev = value

    ranges.append(
        str(start)
        if start == prev
        else f"{start}-{prev}"
    )

    return ", ".join(ranges)


def _fmt_reject_percent(batch: BatchRow, *, defect_count_in_pallet: int) -> str:
    """
    % брака в партии.

    Основной источник — счётчики партии measured_good/measured_bad.
    Если по старым данным они пустые, fallback:
    defect_count_in_pallet / product_count.
    """
    d = batch.data or {}

    mg, mb = _get_counts(batch)
    total = int(mg or 0) + int(mb or 0)

    if total > 0:
        pct = (float(mb or 0) / float(total)) * 100.0
        return f"{pct:.1f}%"

    try:
        product_count = int(d.get("product_count") or 0)
    except Exception:
        product_count = 0

    if product_count > 0:
        pct = (float(defect_count_in_pallet or 0) / float(product_count)) * 100.0
        return f"{pct:.1f}%"

    return "-"


def _build_rejectbin_pallet_protocol_text(
    *,
    reject_bin,
    rows: list[dict],
    count_at_close: int,
    operator_name: str = "-",
) -> str:
    started_at = getattr(reject_bin, "opened_at", None)
    finished_at = getattr(reject_bin, "closed_at", None)

    tare_no = _normalize_rejectbin_tare_no(
        getattr(reject_bin, "tare_no", None)
    )
    tare_no_text = str(tare_no) if tare_no is not None else "-"

    printed_at = _dt_now_local_str()

    header_1 = [
        "Марш.",
        "Наименов.",
        "Обознач.",
        "№ парт.",
        "Кол.",
        "% бр.",
        "Яч.",
        "Яч.",
    ]
    header_2 = [
        "пасп.",
        "",
        "",
        "прут.",
        "бр.",
        "парт.",
        "нач.",
        "кон.",
    ]

    # Общая ширина таблицы примерно 87 символов.
    widths = [8, 10, 9, 7, 4, 5, 4, 4]

    lines: list[str] = [
        f"Протокол заполнения браковочной паллеты №{tare_no_text}",
        "",
        f"Дата начала заполнения:       {_fmt_report_dt(started_at)}",
        f"Дата окончания заполнения:    {_fmt_report_dt(finished_at)}",
        "",
        _table_line(widths),
        _table_row(header_1, widths),
        _table_row(header_2, widths),
        _table_line(widths),
    ]

    if rows:
        for row in rows:
            lines.extend(
                _table_wrapped_rows(
                    [
                        row.get("passport_number", "-"),
                        row.get("product_name", "-"),
                        row.get("product_code", "-"),
                        row.get("rod_batch_number", "-"),
                        row.get("defect_count", "-"),
                        row.get("reject_percent", "-"),
                        row.get("cell_start", "-"),
                        row.get("cell_end", "-"),
                    ],
                    widths,
                )
            )
            lines.append(_table_line(widths))

            unmeasured_positions = row.get(
                "unmeasured_positions"
            ) or []

            if unmeasured_positions:
                lines.append(
                    f"Не измерено (партия {row.get('passport_number', '-')}): "
                    "ячейки "
                    + _fmt_int_ranges(unmeasured_positions)
                )
                lines.append("")            
    else:
        lines.append(
            _table_row(
                [
                    "-",
                    "нет данных",
                    "-",
                    "-",
                    "0",
                    "-",
                    "-",
                    "-",
                ],
                widths,
            )
        )
        lines.append(_table_line(widths))

    lines += [
        "",
        f"Оператор:                     {operator_name}",
        f"Дата и время печати протокола:{printed_at}",
    ]

    return "\n".join(lines)



_PRINT_DOC_ALIASES: dict[str, str] = {
    "summary": "protocol",
    "protocol": "protocol",
    "batch_protocol": "protocol",

    "defect_protocol": "defect_protocol",
    "defect": "defect_protocol",
    "reject_protocol": "defect_protocol",
    "batch_defect_protocol": "defect_protocol",

    "label": "labels",
    "labels": "labels",
}


def _normalize_print_docs(docs: list) -> tuple[list[str], list[str]]:
    normalized: list[str] = []
    unknown: list[str] = []

    for doc in docs:
        raw = str(doc).strip().lower()
        canonical = _PRINT_DOC_ALIASES.get(raw)

        if not canonical:
            unknown.append(str(doc))
            continue

        # один и тот же документ не печатаем дважды из-за алиасов/дублей
        if canonical not in normalized:
            normalized.append(canonical)

    return normalized, unknown


def _enqueue_cmd(db: Session, type_: str, payload: dict):
    c = CommandRow(type=type_, status=CommandStatus.pending.value, payload=(payload or {}))
    db.add(c)
    db.commit()
    db.refresh(c)
    return c


def _set_message(
    db: Session,
    message: str,
    severity: str | Severity = Severity.info.value,
    *,
    message_key: str = "process.notice",
    priority: int | None = None,
    sticky: bool = True,
    ttl_sec: float | None = None,
    refresh_ttl: bool = False,
    **state,
):
    """
    Совместимая точка публикации операторских сообщений.

    Все существующие вызовы без message_key направляются в общий ключ
    process.notice. Legacy-поля SystemState.message и message_severity
    синхронизируются внутренним реестром сообщений в repo.py.
    """
    if state:
        repo.set_state(db, **state)

    return repo.publish_operator_message(
        db,
        message_key=message_key,
        message=message,
        severity=severity,
        priority=priority,
        sticky=sticky,
        ttl_sec=ttl_sec,
        refresh_ttl=refresh_ttl,
    )


def _set_transient_message(
    db: Session,
    message: str,
    severity: str | Severity = Severity.info.value,
    *,
    message_key: str = "notice.general",
    ttl_sec: float | None = None,
    **state,
):
    """
    Публикует краткое подтверждение действия оператора.

    Временные сообщения не участвуют в производственной логике и
    автоматически исчезают. Повторное осознанное действие обновляет TTL.
    """
    severity_value = (
        severity.value
        if hasattr(severity, "value")
        else str(severity)
    )

    if ttl_sec is None:
        ttl_sec = (
            TRANSIENT_WARN_MESSAGE_TTL_SEC
            if severity_value == Severity.warn.value
            else TRANSIENT_INFO_MESSAGE_TTL_SEC
        )

    return _set_message(
        db,
        message,
        severity_value,
        message_key=message_key,
        sticky=False,
        ttl_sec=float(ttl_sec),
        refresh_ttl=False,
        **state,
    )


def _es_status_info(code: int | None) -> str | None:
    if code is None:
        return None

    try:
        code = int(code)
    except Exception:
        return None

    if code == 0:
        return "Аварийный контур восстановлен. Нажмите ПРОДОЛЖИТЬ"
    if code == 1:
        return "Реле безопасности требуется взвод. Нажмите кнопку Сброс Аварии на стойке"

    # ES_status 2..32 = битовая маска code - 1:
    # bit0=Дверь, bit1=К4, bit2=К3, bit3=К2, bit4=К1
    if 2 <= code <= 32:
        mask = code - 1
        parts = []
        if mask & 16:
            parts.append("Ав.Кнопка 1")
        if mask & 8:
            parts.append("Ав.Кнопка 2")
        if mask & 4:
            parts.append("Ав.Кнопка 3")
        if mask & 2:
            parts.append("Ав.Кнопка 4")
        if mask & 1:
            parts.append("Открыта дверь")
        return ", ".join(parts) if parts else "ES_status unknown"

    return f"ES_status unknown ({code})"


class Supervisor:
    def __init__(
        self,
        io: IOBase,
        rtk: RTKPort,
        reject_bin_capacity: int = 50,
        clock: Callable[[], float] = time.time,
        tick_period: float = 1.0,
        printer: Optional[PrinterBackend] = None,
    ):
        self.io = io
        self.rtk = rtk
        self.reject_bin_capacity = int(reject_bin_capacity)
        self.clock = clock
        self._tick_period = float(tick_period)
        self.printer = printer or CupsPrinter()

        # ppod (permissible percentage of defects)
        self._settings_ppod: float = 1.0
        self._ppod_tripped_batch_id: Optional[int] = None

        # Количество браков подряд, после которого запускается проверка.
        # Значение обновляется из settings на каждом tick.
        self._consecutive_rejects_threshold: int = (
            DEFAULT_CONSECUTIVE_REJECTS_THRESHOLD
        )

        self._last_tick = 0.0#: float!!!
        self._resume_target_mode: Optional[str] = None
        
        self._last_need_cycle_on: Optional[bool] = None

        # anti-spam / edge-detection
        self._last_safety_status_code: int | None = None
        self._last_safety_pause_message: str | None = None
        self._last_trash_present: Optional[bool] = None

        self._positioner_missing_emitted: bool = False        

        self._last_air_pressure_ok: Optional[bool] = None
        self._air_pressure_emergency_active: bool = False
        self._air_pressure_lost_since: float | None = None
        self._air_pressure_lost_debounce_sec: float = 1.0                

        self._near_full_emitted: bool = False
        self._rejectbin_full_emitted: bool = False
        
        # reject bin
        self._rejectbin_unload_requested: bool = False

        self._rejectbin_door_open_requested: bool = False
        self._rejectbin_door_open_requested_at: float = 0.0
        self._rejectbin_door_seen_open: bool = False

        # Закрытие принимаем только после устойчивого непрерывного
        # сигнала датчика. Краткий импульс True не должен завершать
        # замену при физически открытой дверце.
        self._rejectbin_door_closed_since: float | None = None
        self._rejectbin_door_close_debounce_sec: float = 2.0

        # True только после того, как во время открытой двери
        # датчик хотя бы один раз показал отсутствие старой тары.
        self._rejectbin_tare_seen_absent: bool = False

        self._rejectbin_reset_done: bool = False
        
        self._rejectbin_unload_reason: str | None = None
        self._rejectbin_reopen_requested_after_missing: bool = False

        self._rejectbin_full_pause_pending: bool = False
        self._rejectbin_full_pause_count: int = 0
        self._rejectbin_full_pause_batch_id: int | None = None                

        # temperature sensors
        self._temp_loading_range: tuple[float, float] = (15.0, 25.0)
        self._temp_im_range: tuple[float, float] = (15.0, 25.0)
        self._temp_violation_emitted: bool = False
        self._temp_near_emitted: bool = False
        self._temp_missing_emitted: bool = False        
        self._temp_last_loading: float | None = None
        self._temp_last_im: float | None = None
        self._temp_last_snapshot_at: float = 0.0        

        # stacklight/sound (int16 nodes)
        self._last_color_cmd: Optional[int] = None
        self._last_sound_cmd: Optional[int] = None

        # one-shot sound events (1/2/3)
        self._pending_sound: int = 0  # 0=no, else 1..3
        self._sound_latch_key: Optional[str] = None  # anti-repeat for event sounds
        self._sound_latched = 0
        self._sound_hold_until = 0.0
        self._auto_start_block_until: float = 0.0

        # warning thresholds (settings)
        self._rjb_near_full: int = 1           # rjb_near_full
        self._temp_near_critical: float = 2.0  # °C

        # RTK wait* sensor handling (delayed poll + send True/False)
        self._wait_sensor_delay_sec: float = 3.0

        self._wait_in_last_send_at: float = 0.0
        self._wait_in_last_state: Optional[bool] = None

        self._wait_out_last_send_at: float = 0.0
        self._wait_out_last_state: Optional[bool] = None

        # wait* attempts: send ONCE per attempt (RTK changes attempt index when switching tare/column)
        self._wait_in_batch_for_attempts: Optional[int] = None
        self._wait_in_sent_attempts: set[int] = set()
        self._wait_in_attempt_id: Optional[int] = None
        self._wait_in_attempt_seen_at: float = 0.0

        self._wait_out_batch_for_attempts: Optional[int] = None
        self._wait_out_sent_attempts: set[int] = set()
        self._wait_out_attempt_id: Optional[int] = None
        self._wait_out_attempt_seen_at: float = 0.0

        self._mm_req_sent_batch: Optional[int] = None

        self._last_im_wait_message: str | None = None
        self._last_im_wait_event_key: str | None = None

        self._calib_req_sent_batch: Optional[int] = None
        self._calib_program_load_req_key: Optional[str] = None

        # После результата -3 действие R2 может остаться тем же
        # waitingcalibrationresult и не дать наблюдаемого edge.
        # Короткий отложенный re-arm используется только как fallback;
        # при обычном выходе из waitingcalibrationresult старый путь
        # продолжает работать без дополнительной задержки.
        self._check_to_calibration_rearm_at: float | None = None
        self._check_to_calibration_rearm_key: tuple[int, str] | None = None

        self._calib_due_at: float | None = None
        self._calib_due_batch: int | None = None
        self._calib_due_etalon_id: int = 0
        self._calib_due_operation_id: str | None = None        
        self._calib_due_reject_threshold: int | None = None

        # per-tare tracking for unloading cell (tareout/putcount)
        self._tare_track_batch_id: int | None = None
        self._tare_last_tareout: int = 0
        self._tare_last_putcount: int = 0        
        self._tare_putcount_base: int = 0
        self._tare_indexing: str | None = None

        # RTK_START сначала только подготавливает tracking.
        # Активируем его после подтверждённой активности нового цикла
        # и подтверждённого сброса putcount.
        self._tare_pending_batch_id: int | None = None
        self._tare_prestart_putcount: int | None = None
        self._tare_putcount_reset_seen: bool = False

    def on_start(self, db: Session):
        st = repo.ensure_state_row(db)

        # После рестарта daemon аварийный звук снова разрешён.
        # Это не позволит сохранить случайно заглушённый sound=4
        # после перезапуска системы.
        repo.set_setting(db, EMERGENCY_ACTIVE_SETTING, False)
        repo.set_setting(db, EMERGENCY_SOUND_MUTED_SETTING, False)        
    
        try:
            repo.ensure_active_reject_bin(db, self.reject_bin_capacity)
        except Exception:
            pass
    
        safety_ok = self.io.read_safety_ok()
        trash_present = self.io.read_trashcan_present()
    
        # не перетираем mode/active_batch на рестарте
        prev_mode = st.mode
        _set_transient_message(
            db,
            "Система готова к работе",
            Severity.info,
            message_key="notice.system",
            reject_bin_capacity=self.reject_bin_capacity,
            safety_ok=1 if safety_ok else 0,
            trash_present=1 if trash_present else 0,
        )
        repo.add_event(
            db,
            severity=Severity.info.value,
            source="DAEMON",
            type_=EventType.DAEMON_STARTED.value,
            payload={"prev_mode": prev_mode},
        )
    
        self._last_trash_present = trash_present
    
        # восстановление состояния auto после рестарта
        self._recover_after_restart(db)


    # ---------- internal helpers ----------
    def _defect_protocol_text_for_batch(
        self,
        db: Session,
        batch: BatchRow,
        *,
        operator_name: str = "-",
    ) -> str | None:
        batch_items = db.execute(
            select(RejectBinItemRow)
            .where(RejectBinItemRow.batch_id == int(batch.id))
            .order_by(RejectBinItemRow.id.asc())
        ).scalars().all()
        if not batch_items:
            return None
    
        bin_ids = sorted({int(it.reject_bin_id) for it in batch_items if it.reject_bin_id is not None})
        if not bin_ids:
            return None
    
        all_items = db.execute(
            select(RejectBinItemRow)
            .where(RejectBinItemRow.reject_bin_id.in_(bin_ids))
            .order_by(RejectBinItemRow.reject_bin_id.asc(), RejectBinItemRow.id.asc())
        ).scalars().all()

        reject_bins = db.execute(
            select(RejectBinRow)
            .where(RejectBinRow.id.in_(bin_ids))
        ).scalars().all()

        tare_no_by_bin_id = {
            int(reject_bin.id): _normalize_rejectbin_tare_no(
                getattr(reject_bin, "tare_no", None)
            )
            for reject_bin in reject_bins
        }
    
        return _build_defect_protocol_text(
            batch,
            all_items,
            tare_no_by_bin_id=tare_no_by_bin_id,
            operator_name=operator_name,
        )    


    def _rejectbin_pallet_protocol_text(
        self,
        db: Session,
        reject_bin,
        *,
        count_at_close: int,
    ) -> str:
        reject_bin_id = int(getattr(reject_bin, "id"))

        items = db.execute(
            select(RejectBinItemRow)
            .where(RejectBinItemRow.reject_bin_id == reject_bin_id)
            .order_by(RejectBinItemRow.id.asc())
        ).scalars().all()

        # Позиция детали внутри браковочной паллеты:
        # первая записанная деталь = ячейка 1, следующая = 2 и т.д.
        positions_by_batch: dict[int, list[int]] = {}
        unmeasured_positions_by_batch: dict[int, list[int]] = {}        

        for pos, item in enumerate(items, start=1):
            try:
                batch_id = int(item.batch_id or 0)
            except Exception:
                batch_id = 0

            if batch_id <= 0:
                continue

            positions_by_batch.setdefault(batch_id, []).append(int(pos))

            measured_params = item.measured_params or {}
            if (
                isinstance(measured_params, dict)
                and bool(measured_params.get("unmeasured"))
            ):
                unmeasured_positions_by_batch.setdefault(
                    batch_id,
                    [],
                ).append(int(pos))            

        batch_ids = sorted(positions_by_batch.keys())
        batch_map: dict[int, BatchRow] = {}

        if batch_ids:
            batches = db.execute(
                select(BatchRow)
                .where(BatchRow.id.in_(batch_ids))
            ).scalars().all()

            batch_map = {
                int(batch.id): batch
                for batch in batches
            }

        rows: list[dict] = []

        for batch_id in batch_ids:
            batch = batch_map.get(int(batch_id))
            positions = positions_by_batch.get(int(batch_id)) or []

            if not positions:
                continue

            data = dict(batch.data or {}) if batch else {}

            defect_count = len(positions)

            rows.append({
                "batch_id": int(batch_id),
                "passport_number": data.get("passport_number") or "-",
                "product_name": data.get("product_name") or "-",
                "product_code": data.get("product_code") or "-",
                "rod_batch_number": data.get("rod_batch_number") or "-",
                "defect_count": int(defect_count),
                "reject_percent": (
                    _fmt_reject_percent(
                        batch,
                        defect_count_in_pallet=int(defect_count),
                    )
                    if batch
                    else "-"
                ),
                "cell_start": min(positions),
                "cell_end": max(positions),
                "unmeasured_positions": (
                    unmeasured_positions_by_batch.get(
                        int(batch_id),
                        [],
                    )
                ),                
            })

        return _build_rejectbin_pallet_protocol_text(
            reject_bin=reject_bin,
            rows=rows,
            count_at_close=int(count_at_close or len(items)),
            operator_name=str(
                repo.get_setting(
                    db,
                    REJECT_BIN_REPLACEMENT_OPERATOR_SETTING,
                    "-",
                )
                or "-"
            ),
        )


    def _tare_idx_to_no(self, idx: int) -> int:
        # idx -> номер тары для печати (1..N)
        if self._tare_indexing == "one":
            return int(idx)
        if self._tare_indexing == "zero":
            return int(idx) + 1
        # fallback: если idx>0 — почти всегда это уже номер тары
        return int(idx) if int(idx) > 0 else 1
    
    
    def _reset_tare_tracking(
        self,
        *,
        reason: str,
        batch_id: int | None = None,
    ) -> None:
        # reason/batch_id оставлены в сигнатуре, чтобы границы сброса
        # tracking были явно видны в местах вызова.
        self._tare_track_batch_id = None
        self._tare_last_tareout = 0
        self._tare_last_putcount = 0
        self._tare_putcount_base = 0
        self._tare_indexing = None
        self._tare_pending_batch_id = None
        self._tare_prestart_putcount = None
        self._tare_putcount_reset_seen = False


    def _maybe_finalize_open_tare(
        self,
        db: Session,
        b: "BatchRow",
        *,
        reason: str,
    ) -> bool:
        # Финализируем текущую тару только для её tracking-контекста.
        if not b:
            return False

        batch_id = int(b.id)

        if self._tare_track_batch_id != batch_id:
            return False

        last_idx = int(self._tare_last_tareout or 0)
        last_pc = int(self._tare_last_putcount or 0)
        base_pc = int(self._tare_putcount_base or 0)

        tare_no = self._tare_idx_to_no(last_idx)
        qty = last_pc - base_pc

        if qty <= 0 or tare_no <= 0:
            return False

        cell_no_i = _first_out_cell(b)
        existing = _norm_out_tares(b)

        if any(
            x["cell_no"] == cell_no_i
            and x["tare_no"] == tare_no
            for x in existing
        ):
            return False

        _upsert_out_tare(
            b,
            cell_no=cell_no_i,
            tare_no=tare_no,
            qty=int(qty),
        )
        db.commit()
        return True


    def _refresh_runtime_settings(self, db: Session):
        st = repo.ensure_state_row(db)
    
        cap = int(st.reject_bin_capacity or 0)
        if cap >= 0 and cap != self.reject_bin_capacity:
            self.reject_bin_capacity = cap
    
        tp = repo.get_setting(db, "tick_period", self._tick_period)
        try:
            tp = float(tp)
            if tp > 0:
                self._tick_period = tp
        except Exception:
            pass
    
        # rjb_near_full
        try:
            v = repo.get_setting(db, "rjb_near_full", self._rjb_near_full)
            v = int(float(v))
            if v < 0:
                v = 0
            self._rjb_near_full = v
        except Exception:
            pass

        # ppod
        pp = repo.get_setting(db, "settings_ppod", self._settings_ppod)
        try:
            pp = float(pp)
            if pp < 0:
                pp = 0.0
            if pp > 1:
                pp = 1.0
            self._settings_ppod = pp
        except Exception:
            pass

        # Количество браков подряд до запуска проверки.
        try:
            raw_threshold = repo.get_setting(
                db,
                "consecutive_rejects_threshold",
                self._consecutive_rejects_threshold,
            )

            if isinstance(raw_threshold, bool):
                raise ValueError("boolean is not an integer threshold")

            threshold_number = float(raw_threshold)
            if (
                not math.isfinite(threshold_number)
                or not threshold_number.is_integer()
                or threshold_number <= 0
            ):
                raise ValueError("threshold must be a positive integer")

            self._consecutive_rejects_threshold = int(
                threshold_number
            )
        except (TypeError, ValueError):
            # Некорректное значение в БД не должно менять проверенную
            # производственную логику: сохраняем последнее валидное значение.
            pass

        # temp_near_critical (°C)
        try:
            v = repo.get_setting(db, "temp_near_critical", self._temp_near_critical)
            v = float(v)
            if v < 0:
                v = 0.0
            self._temp_near_critical = v
        except Exception:
            pass

        # temperature ranges
        t1_min = repo.get_setting(db, "temperature_sensor_loading_min")
        t1_max = repo.get_setting(db, "temperature_sensor_loading_max")
        t2_min = repo.get_setting(db, "temperature_sensor_im_min")
        t2_max = repo.get_setting(db, "temperature_sensor_im_max")
        try:
            if t1_min is not None and t1_max is not None:
                self._temp_loading_range = (float(t1_min), float(t1_max))
            if t2_min is not None and t2_max is not None:
                self._temp_im_range = (float(t2_min), float(t2_max))
        except Exception:
            pass


    def _build_temperature_state(
        self,
        t_loading,
        t_im,
    ) -> dict:
        """
        Формирует единый оперативный snapshot температуры.

        Значения температуры не сохраняются в БД. Snapshot используется:
        - для логики паузы/старта;
        - для событий переходов OK/near/error;
        - для отображения в workplace через небольшой runtime JSON-файл.
        """
        loading_min, loading_max = self._temp_loading_range
        im_min, im_max = self._temp_im_range
        delta = max(0.0, float(self._temp_near_critical or 0.0))

        def _sensor(raw, value_min: float, value_max: float) -> dict:
            value = None

            if raw is not None:
                try:
                    candidate = float(raw)
                    if math.isfinite(candidate):
                        value = candidate
                except (TypeError, ValueError):
                    value = None

            if value is None:
                status = "missing"
            elif value < float(value_min) or value > float(value_max):
                status = "out_of_range"
            elif delta > 0.0 and (
                (value - float(value_min) <= delta)
                or (float(value_max) - value <= delta)
            ):
                status = "near_critical"
            else:
                status = "ok"

            return {
                "value": value,
                "status": status,
                "min": float(value_min),
                "max": float(value_max),
            }

        loading = _sensor(t_loading, loading_min, loading_max)
        im = _sensor(t_im, im_min, im_max)
        statuses = {loading["status"], im["status"]}

        if "missing" in statuses:
            overall_status = "missing"
        elif "out_of_range" in statuses:
            overall_status = "out_of_range"
        elif "near_critical" in statuses:
            overall_status = "near_critical"
        else:
            overall_status = "ok"

        return {
            "overall_status": overall_status,
            "near_critical_delta": delta,
            "loading": loading,
            "im": im,
        }


    def _read_temperature_state(self) -> dict:
        return self._build_temperature_state(
            self.io.read_temperature_sensor_loading(),
            self.io.read_temperature_sensor_im(),
        )


    def _temperature_message(
        self,
        temperature_state: dict,
        status: str,
    ) -> str:
        labels = {
            "loading": "постамат",
            "im": "измерительная машина",
        }
        parts: list[str] = []

        for key in ("loading", "im"):
            sensor = temperature_state.get(key) or {}
            if str(sensor.get("status") or "") != str(status):
                continue

            label = labels[key]
            value = sensor.get("value")
            value_min = float(sensor.get("min"))
            value_max = float(sensor.get("max"))

            if status == "missing":
                parts.append(label)
            else:
                parts.append(
                    f"{label}: {float(value):.1f} °C "
                    f"[{value_min:.1f}..{value_max:.1f}]"
                )

        if status == "missing":
            return "Отсутствует значение температуры: " + ", ".join(parts)

        if status == "out_of_range":
            return "Температура вне допустимого диапазона: " + "; ".join(parts)

        if status == "near_critical":
            return "Температура близка к критической: " + "; ".join(parts)

        return "Температура в норме"


    def _temperature_event_payload(
        self,
        temperature_state: dict,
        *,
        where: str,
    ) -> dict:
        return {
            "where": str(where),
            "overall_status": temperature_state.get("overall_status"),
            "near_critical_delta": temperature_state.get("near_critical_delta"),
            "loading": dict(temperature_state.get("loading") or {}),
            "im": dict(temperature_state.get("im") or {}),
        }


    def _update_temperature_events(
        self,
        db: Session,
        temperature_state: dict,
        *,
        where: str,
        publish_near_message: bool,
        force: bool = False,
    ) -> None:
        """
        Синхронизирует независимое сообщение ``temperature.state``
        с текущим физическим состоянием датчиков.

        События остаются edge-triggered, а sticky-сообщение существует
        всё время, пока сохраняется missing/out_of_range/near_critical.
        При полном возврате в норму удаляется только temperature.state.
        """
        _ = publish_near_message  # сохранено для совместимости вызовов

        status = str(
            temperature_state.get("overall_status")
            or "missing"
        )
        payload = self._temperature_event_payload(
            temperature_state,
            where=where,
        )

        if status == "missing":
            message = self._temperature_message(
                temperature_state,
                "missing",
            )
            self._set_message_if_changed(
                db,
                message,
                Severity.error,
                message_key="temperature.state",
            )

            self._temp_violation_emitted = False
            self._temp_near_emitted = False

            if force or not self._temp_missing_emitted:
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.TEMPERATURE_UNAVAILABLE.value,
                    payload={**payload, "message": message},
                )
                self._temp_missing_emitted = True
            return

        if status == "out_of_range":
            message = self._temperature_message(
                temperature_state,
                "out_of_range",
            )
            self._set_message_if_changed(
                db,
                message,
                Severity.error,
                message_key="temperature.state",
            )

            self._temp_missing_emitted = False
            self._temp_near_emitted = False

            if force or not self._temp_violation_emitted:
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.TEMPERATURE_OUT_OF_RANGE.value,
                    payload={**payload, "message": message},
                )
                self._temp_violation_emitted = True
            return

        if status == "near_critical":
            message = self._temperature_message(
                temperature_state,
                "near_critical",
            )
            self._set_message_if_changed(
                db,
                message,
                Severity.warn,
                message_key="temperature.state",
            )

            self._temp_missing_emitted = False
            self._temp_violation_emitted = False

            if force or not self._temp_near_emitted:
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.TEMPERATURE_NEAR_CRITICAL.value,
                    payload={
                        **payload,
                        "message": message,
                    },
                )
                self._temp_near_emitted = True
            return

        repo.clear_operator_message(
            db,
            "temperature.state",
        )
        self._temp_missing_emitted = False
        self._temp_violation_emitted = False
        self._temp_near_emitted = False


    def _publish_temperature_snapshot(
        self,
        *,
        now: float,
        temperature_state: dict,
    ) -> None:
        """
        Публикует только оперативный snapshot для API/workplace.
        База данных не используется.
        """
        if not TEMPERATURE_SNAPSHOT_FILE:
            return

        if (
            float(now) - float(self._temp_last_snapshot_at)
            < float(TEMPERATURE_SNAPSHOT_PERIOD_SEC)
        ):
            return

        payload = {
            "updated_at_ts": float(time.time()),
            **temperature_state,
        }

        path = str(TEMPERATURE_SNAPSHOT_FILE)
        tmp_path = f"{path}.{os.getpid()}.tmp"

        try:
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(
                    payload,
                    fh,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

            os.replace(tmp_path, path)
            self._temp_last_snapshot_at = float(now)

        except Exception:
            # Ошибка публикации snapshot не должна останавливать supervisor.
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass


    def _recover_after_restart(self, db: Session):
        """Сводим SystemStateRow с фактом в batches (auto_processing) после рестарта."""
        st = repo.ensure_state_row(db)
    
        # если active_batch_id указывает на невалидную партию - чистим
        if st.active_batch_id is not None:
            b = repo.get_batch(db, int(st.active_batch_id))
            if (b is None) or (getattr(b, "status", None) != "auto_processing"):
                _set_transient_message(
                    db,
                    "После перезапуска сброшена устаревшая ссылка на активную партию",
                    Severity.warn,
                    message_key="notice.recovery",
                    active_batch_id=None,
                    active_batch_phase=None,
                    active_batch_expected_count=None,
                    active_operation_id=None,
                    active_operation_batch_id=None,
                    active_operation_phase=None,
                    active_operation_started_at=None,                    
                    rtk_pickcount_seen=None,
                    rtk_defectcount_seen=None,
                    rtk_consecutive_defects=0,
                )
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={"active_batch_id": None},
                )
                st = repo.ensure_state_row(db)
    
        autos = repo.list_batches_by_status(db, "auto_processing", limit=3)
        if not autos:
            return
    
        if len(autos) > 1:
            repo.add_event(
                db,
                severity=Severity.error.value,
                source="DAEMON",
                type_=EventType.ERROR.value,
                payload={"where": "RECOVER_AUTO", "err": "multiple auto_processing", "batch_ids": [b.id for b in autos]},
            )
    
        b0 = autos[0]
        if st.active_batch_id is None or int(st.active_batch_id) != int(b0.id):
            expected = st.active_batch_expected_count
            if not expected:
                try:
                    expected = int((b0.data or {}).get("product_count") or (b0.data or {}).get("ProductCount") or 0)
                except Exception:
                    expected = None

            operation_id = (
                str(getattr(st, "active_operation_id", "") or "").strip()
                or repo.new_operation_id()
            )

            recovered_data = dict(b0.data or {})
            recovered_phase = (
                st.active_batch_phase
                or (
                    STOP_CLEANUP_PHASE
                    if bool(
                        recovered_data.get(
                            "stop_cleanup_active"
                        )
                    )
                    else "starting"
                )
            )

            current_operation_phase = str(
                getattr(
                    st,
                    "active_operation_phase",
                    "",
                )
                or ""
            ).strip()

            if recovered_phase == STOP_CLEANUP_PHASE:
                recovered_operation_phase = None

            elif current_operation_phase:
                recovered_operation_phase = (
                    current_operation_phase
                )

            elif recovered_phase in {
                "running",
                "finishing",
            }:
                recovered_operation_phase = (
                    recovered_phase
                )

            else:
                # auto_processing уже создана, но физическая
                # активность роботов ещё должна подтвердиться.
                recovered_operation_phase = (
                    "rtk_start_sent"
                )            
    
            _set_transient_message(
                db,
                f"После перезапуска восстановлена активная партия {b0.id}",
                Severity.info,
                message_key="notice.recovery",
                active_batch_id=int(b0.id),
                active_batch_phase=recovered_phase,
                active_batch_expected_count=expected,
                active_operation_id=(
                    None
                    if recovered_phase == STOP_CLEANUP_PHASE
                    else operation_id
                ),
                active_operation_batch_id=(
                    None
                    if recovered_phase == STOP_CLEANUP_PHASE
                    else int(b0.id)
                ),
                active_operation_phase=(
                    recovered_operation_phase
                ),
                active_operation_started_at=(
                    None
                    if recovered_phase == STOP_CLEANUP_PHASE
                    else utcnow()
                ),                
                rtk_pickcount_seen=None,
                rtk_defectcount_seen=None,
                rtk_consecutive_defects=0,
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "active_batch_id": int(b0.id),
                    "active_operation_id": operation_id,                    
                    "active_batch_expected_count": expected,
                },
            )

    def _remember_resume_target(self, current_mode: str):
        if current_mode not in (
            SystemMode.paused_safety.value,
            SystemMode.paused_rejectbin.value,
            SystemMode.paused_trash_missing.value,
            SystemMode.paused_temperature.value,
            SystemMode.paused_no_air.value,
        ):
            self._resume_target_mode = current_mode

    def _pause_to(
        self,
        db: Session,
        new_mode: str,
        message: str,
        event_type: str,
        severity: str,
        *,
        message_key: str = "process.notice",
    ):
        st = repo.ensure_state_row(db)
        self._remember_resume_target(st.mode)

        if st.mode != new_mode:
            _set_message(
                db,
                message,
                severity,
                message_key=message_key,
                mode=new_mode,
            )
            repo.add_event(
                db,
                severity=severity,
                source="DAEMON",
                type_=event_type,
                payload={},
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={"mode": new_mode},
            )

        # если паузим именно автоцикл - паузим и RTK
        if self._resume_target_mode == SystemMode.auto_running.value:
            if new_mode == SystemMode.paused_safety.value:
                self._rtk_pause(db, "safety")
            elif new_mode == SystemMode.paused_trash_missing.value:
                self._rtk_pause(db, "trash_missing")
            elif new_mode == SystemMode.paused_rejectbin.value:
                self._rtk_pause(db, "rejectbin_full")
            elif new_mode == SystemMode.paused_temperature.value:
                self._rtk_pause(db, "temperature_error")
            elif new_mode == SystemMode.paused_no_air.value:
                self._rtk_pause(db, "no_air")


    def _io_connected(self) -> bool:
        attr = getattr(self.io, "connected", None)
        if attr is None:
            return True  # mock/local backends без connected считаем доступными
        try:
            return bool(attr() if callable(attr) else attr)
        except Exception:
            return False 
    
    def _io_error(self) -> str | None:
        try:
            err = getattr(self.io, "last_error", None)
            err = err() if callable(err) else err
            return str(err) if err else None
        except Exception:
            return None
    
    def _postamat_offline_problem(self, fallback: str) -> str | None:
        if self._io_connected():
            return None
    
        err = self._io_error()
        suffix = f" ({err})" if err else ""
        return f"{fallback}: нет связи с постаматами; аварийный контур недоступен{suffix}"

    def _read_safety_status(self) -> tuple[int | None, str | None]:
        reader = getattr(self.io, "read_safety_status", None)
        if not callable(reader):
            return None, None

        try:
            code = reader()
        except Exception:
            return None, None

        if code is None:
            return None, None

        try:
            code_i = int(code)
        except Exception:
            return None, None

        return code_i, _es_status_info(code_i)

    def _format_safety_problem(self, fallback: str) -> str:
        offline = self._postamat_offline_problem(fallback)
        if offline:
            return offline

        code, info = self._read_safety_status()
        if code is None:
            return fallback

        if info:
            return f"{fallback} {info} (код={code})"

        return f"{fallback} (код={code})"


    def _air_pressure_ok_from_snapshot(self, snap) -> bool:
        """
        Давление воздуха приходит с РТК: Data/io/DI09.
        True  = давление в норме. False = авария.  
        Если РТК не подключён - не дублируем ошибку воздуха:
        это уже обрабатывается как ошибка связи с РТК.
        Если поле есть, но значение None при подключённом РТК - fail-safe NOK.
        """
        if snap is None or not bool(getattr(snap, "connected", False)):
            return True
    
        v = getattr(snap, "air_pressure_ok", True)
    
        if v is None:
            return True
    
        return bool(v)
    
    
    def _format_air_pressure_problem(self) -> str:
        return "АВАРИЯ: нет давления воздуха"


    def _update_safety_pause_message(
        self,
        db: Session,
        *,
        safety_msg: str,
        safety_status_code: int | None,
        safety_status_info: str | None,
    ) -> None:
        current = next(
            (
                item
                for item in repo.list_operator_messages(db)
                if item.get("key") == "safety.circuit"
            ),
            None,
        )

        if (
            self._last_safety_pause_message == safety_msg
            and current is not None
            and current.get("message") == safety_msg
            and current.get("severity") == Severity.error.value
        ):
            return

        _set_message(
            db,
            safety_msg,
            Severity.error,
            message_key="safety.circuit",
        )
        repo.add_event(
            db,
            severity=Severity.error.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": safety_msg,
                "message_key": "safety.circuit",
                "safety_status_code": safety_status_code,
                "safety_status_info": safety_status_info,
            },
        )
        self._last_safety_pause_message = safety_msg


    def _can_resume(self, db: Session) -> tuple[bool, Optional[str]]:
        if not self._io_connected():
            return False, self._format_safety_problem("safety is FALSE")

        safety_ok = self.io.read_safety_ok()
        if not safety_ok:
            return False, self._format_safety_problem("safety is FALSE")

        if bool(repo.get_setting(db, "equipment_pause_active", False)):
            reason = str(
                repo.get_setting(db, "equipment_pause_reason", "") or ""
            ).strip()
            suffix = f": {reason}" if reason else ""
            return False, "Ожидание восстановления связи с оборудованием" + suffix

        trash_present = self.io.read_trashcan_present()
        if not trash_present:
            return False, "Отсутствует тара брака"

        snap = self.rtk.snapshot()
        if not self._air_pressure_ok_from_snapshot(snap):
            return False, "Нет давления воздуха"

        temperature_state = self._read_temperature_state()
        temperature_status = str(
            temperature_state.get("overall_status")
            or "missing"
        )

        if temperature_status == "missing":
            return False, self._temperature_message(
                temperature_state,
                "missing",
            )

        if temperature_status == "out_of_range":
            return False, self._temperature_message(
                temperature_state,
                "out_of_range",
            )       

        st = repo.ensure_state_row(db)
        reject_count = int(st.reject_bin_count or 0)
        if (
            self.reject_bin_capacity > 0
            and reject_count >= self.reject_bin_capacity
            and not bool(getattr(self, "_rejectbin_reset_done", False))
        ):
            return False, "reject bin is full"
    
        return True, None


    def _queue_sound(self, now: float, code: int) -> None:
        code = int(code)
        if code <= 0:
            return
    
        hold = {1: 2.5, 2: 2.5, 3: 4.5}.get(code, 2.0)  #2.0 подстроить под реальные длительности
        # если уже стоит этот же код и ещё держим - не перезапускаем
        if self._sound_latched == code and now < self._sound_hold_until:
            return
    
        self._sound_latched = code
        self._sound_hold_until = now + hold

    def _flush_signals(self, *, color: int, sound: int) -> None:
        try:
            if self._last_color_cmd != int(color):
                self.io.request_stacklight_color(int(color))
                self._last_color_cmd = int(color)
        except Exception as e:
            pass

        try:
            if self._last_sound_cmd != int(sound):
                self.io.request_stacklight_sound(int(sound))
                self._last_sound_cmd = int(sound)
        except Exception as e:
            pass

    def _clear_ppod_latch(self, db: Session, *, reason: str):
        if self._ppod_tripped_batch_id is None:
            return

        old_id = int(self._ppod_tripped_batch_id)
        self._ppod_tripped_batch_id = None

        repo.add_event(
            db,
            severity=Severity.info.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": "ppod latch cleared",
                "reason": str(reason),
                "batch_id": old_id,
            },
        )

    def _apply_signals(
        self,
        db: Session,
        *,
        now: float,
        mode: str,
        safety_ok: bool,
        trash_present: bool,
        reject_count: int,
        cs_r1: Optional[bool] = None,
        cs_r2: Optional[bool] = None,
        system_warn: bool = False,
        ppod_tripped: bool = False,
        temperature_bad: bool = False,
        air_pressure_ok: bool = True,
    ):
        reject_full = self.reject_bin_capacity > 0 and reject_count >= self.reject_bin_capacity
        is_auto = (mode == SystemMode.auto_running.value)
        is_paused = mode.startswith("paused")

        cs_bad = (cs_r1 is False) or (cs_r2 is False)

        # --- determine "emergency" ---
        # авария: safety/trash/temperature/прочие паузы кроме заполненного брака.
        # PPOD — это браковка партии, а не аппаратная авария:
        # звук не должен висеть как sound=4.
        is_reject_pause = (mode == SystemMode.paused_rejectbin.value) or (is_paused and reject_full)
        ppod_active = bool(ppod_tripped)

        air_recovered_waiting_resume = (
            mode == SystemMode.paused_no_air.value
            and bool(air_pressure_ok)
        )

        emergency = (
            (not safety_ok)
            or (not trash_present)
            or bool(temperature_bad)
            or (not air_pressure_ok)
            or (
                is_paused
                and not is_reject_pause
                and not air_recovered_waiting_resume
            )
        )

        emergency_sound_muted = bool(
            repo.get_setting(
                db,
                EMERGENCY_SOUND_MUTED_SETTING,
                False,
            )
        )
        
        # После устранения аварии снимаем операторское подавление звука.
        # Следующее emergency всегда должно снова начаться со sound=4.
        if not emergency and emergency_sound_muted:
            emergency_sound_muted = False
        
            repo.set_setting(
                db,
                EMERGENCY_SOUND_MUTED_SETTING,
                False,
            )
        
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": "emergency sound mute reset",
                    "emergency_active": False,
                },
            )
        
        # Публикуем фактическое состояние emergency для интерфейса.
        emergency_active_prev = bool(
            repo.get_setting(
                db,
                EMERGENCY_ACTIVE_SETTING,
                False,
            )
        )
        
        if emergency_active_prev != bool(emergency):
            repo.set_setting(
                db,
                EMERGENCY_ACTIVE_SETTING,
                bool(emergency),
            )        

        # --- color (int16) ---
        # priority top-down
        if emergency:
            color = 3  # red steady
        elif ppod_active:
            color = 4  # yellow<->red, как у полной тары брака
        elif is_reject_pause:
            color = 4  # yellow<->red
        elif cs_bad:
            color = 6  # yellow blinking
        elif is_auto and system_warn:
            color = 5  # yellow<->green
        elif is_auto:
            color = 2  # green steady
        else:
            color = 1  # idle = yellow steady

        # --- sound (int16) ---
        # priority: emergency(4) > rejectbin pause(3) > one-shot latch(1/2) > silent(0)
        sound = 0

        if emergency:
            sound = 0 if emergency_sound_muted else 4
        
            # одноразовые сигналы в аварии не нужны
            self._sound_latched = 0
            self._sound_hold_until = 0.0

        elif ppod_active or is_reject_pause:
            # PPOD и полная тара брака: три продолжительных сигнала
            sound = 3
            # не смешиваем с одноразовыми (1/2)
            self._sound_latched = 0
            self._sound_hold_until = 0.0

        else:
            # если есть “одноразовый” сигнал (1/2) — удерживаем
            if self._sound_latched:
                # статус может отсутствовать — тогда просто держим по времени
                try:
                    st = int(self.io.read_stacklight_sound_status() or 0)
                except Exception:
                    st = None

                if (now < self._sound_hold_until) or (st not in (None, 0)):
                    sound = self._sound_latched
                else:
                    # PLC уже “молчит” и таймер выдержан — можно сбросить в 0
                    self._sound_latched = 0
                    self._sound_hold_until = 0.0
                    sound = 0

        # flush to IO (new nodes)
        self._flush_signals(color=color, sound=sound)

        # log only on change
        if getattr(self, "_last_signals_logged", None) != (color, sound):
            self._last_signals_logged = (color, sound)
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.SIGNALS_APPLIED.value,
                payload={"mode": mode, "color": int(color), "sound": int(sound)},
            )


    def _rtk_pause(self, db: Session, reason: str):
        st = repo.ensure_state_row(db)
        if bool(getattr(st, "rtk_paused", 0)):
            return
        snap = self.rtk.snapshot()
        if not snap.connected:
            return False, "rtk not connected"
    
        self.rtk.request("pause", {"reason": reason})
        repo.set_state(db, rtk_paused=1, rtk_pause_reason=reason)
        repo.add_event(db, severity=Severity.warn.value, source="DAEMON",
                       type_=EventType.RTK_COMMAND_SENT.value,
                       payload={"cmd": "pause", "reason": reason})
    
    
    def _rtk_resume(self, db: Session):
        st = repo.ensure_state_row(db)
        if not bool(getattr(st, "rtk_paused", 0)):
            return
        snap = self.rtk.snapshot()
        if not snap.connected:
            return
    
        self.rtk.request("resume", {"reason": st.rtk_pause_reason or "resume"})
        repo.set_state(db, rtk_paused=0, rtk_pause_reason=None)
        repo.add_event(db, severity=Severity.info.value, source="DAEMON",
                       type_=EventType.RTK_COMMAND_SENT.value,
                       payload={"cmd": "resume"})
    
    
    def _rtk_stop(self, db: Session, reason: str):
        snap = self.rtk.snapshot()
        if not snap.connected:
            return
    
        self.rtk.request("stop", {"reason": reason})
        repo.set_state(db, rtk_paused=0, rtk_pause_reason=None)
        repo.add_event(db, severity=Severity.error.value, source="DAEMON",
                       type_=EventType.RTK_COMMAND_SENT.value,
                       payload={"cmd": "stop", "reason": reason})


    def _rtk_cycle_on_robot_diagnostics(
        self,
        snap,
        *,
        suffix: str,
        label: str,
    ) -> tuple[list[str], list[str], dict]:
        """
        Возвращает причины, мешающие включению cycle_on для робота,
        и полный набор исходных диагностических значений для события.

        Для готовности ожидаются:
        teach/teachl/tpemg/opemg/exemg/error == False, ecode == 0.
        Сам cs может оставаться False до отправки команды cycle_on.
        """
        cs = getattr(snap, f"cs_{suffix}", None)

        values = {
            "cs": cs,
            "teach": getattr(snap, f"teach_{suffix}", None),
            "teachl": getattr(snap, f"teachl_{suffix}", None),
            "tpemg": getattr(snap, f"tpemg_{suffix}", None),
            "opemg": getattr(snap, f"opemg_{suffix}", None),
            "exemg": getattr(snap, f"exemg_{suffix}", None),
            "error": getattr(snap, f"robot_error_{suffix}", None),
            "ecode": getattr(snap, f"ecode_{suffix}", None),
        }

        reasons: list[str] = []
        missing: list[str] = []

        expected_false = (
            ("teach", "контроллер робота находится в режиме обучения"),
            ("teachl", "пульт робота находится в режиме обучения"),
            ("tpemg", "нажата аварийная кнопка на пульте"),
            ("opemg", "нажата аварийная кнопка на контроллере"),
            ("exemg", "нажата внешняя аварийная кнопка"),
        )

        for key, text in expected_false:
            value = values[key]
            if value is True:
                reasons.append(f"{label}: {text} ({key}=true)")
            elif value is None:
                missing.append(key)

        robot_error = values["error"]
        ecode = values["ecode"]

        if robot_error is True:
            code_text = (
                f", ecode={ecode}"
                if ecode is not None
                else ""
            )
            reasons.append(
                f"{label}: ошибка на роботе "
                f"(error=true{code_text})"
            )
        elif robot_error is None:
            missing.append("error")

        if ecode is None:
            missing.append("ecode")
        else:
            try:
                ecode_i = int(ecode)
            except (TypeError, ValueError):
                ecode_i = None

            if ecode_i is None:
                missing.append("ecode")
            elif ecode_i != 0 and robot_error is not True:
                reasons.append(
                    f"{label}: ненулевой код ошибки "
                    f"(ecode={ecode_i})"
                )

        if cs is None:
            reasons.append(
                f"{label}: не получен флаг готовности цикла cs"
            )

        return reasons, sorted(set(missing)), values


    def _rtk_r2_action(self) -> str:
        try:
            snap = self.rtk.snapshot()
            return str(
                getattr(snap, "action_r2", "")
                or getattr(snap, "state", "")
                or ""
            ).strip().lower()
        except Exception:
            return ""


    def _rtk_r2_external_wait_state(self) -> str | None:
        a2 = self._rtk_r2_action()
        if a2 in {"waitingmmresult", "waitingcalibrationresult"}:
            return a2
        return None


    def _calibration_retry_safety_gate_matches(
        self,
        db: Session,
        *,
        batch_id: int | None,
        operation_id: str | None,
    ) -> bool:
        try:
            pending_batch_id = int(
                repo.get_setting(
                    db,
                    CALIBRATION_RETRY_SAFETY_BATCH_SETTING,
                    0,
                )
                or 0
            )
        except Exception:
            pending_batch_id = 0

        pending_operation_id = str(
            repo.get_setting(
                db,
                CALIBRATION_RETRY_SAFETY_OPERATION_SETTING,
                "",
            )
            or ""
        ).strip()

        try:
            current_batch_id = int(batch_id or 0)
        except Exception:
            current_batch_id = 0

        current_operation_id = str(operation_id or "").strip()

        return bool(
            pending_batch_id > 0
            and current_batch_id == pending_batch_id
            and pending_operation_id
            and current_operation_id == pending_operation_id
        )


    def _set_calibration_retry_safety_gate(
        self,
        db: Session,
        *,
        batch_id: int | None,
        operation_id: str | None,
    ) -> None:
        repo.set_setting(
            db,
            CALIBRATION_RETRY_SAFETY_BATCH_SETTING,
            int(batch_id or 0),
        )
        repo.set_setting(
            db,
            CALIBRATION_RETRY_SAFETY_OPERATION_SETTING,
            str(operation_id or ""),
        )


    def _clear_calibration_retry_safety_gate(
        self,
        db: Session,
    ) -> None:
        repo.set_setting(
            db,
            CALIBRATION_RETRY_SAFETY_BATCH_SETTING,
            0,
        )
        repo.set_setting(
            db,
            CALIBRATION_RETRY_SAFETY_OPERATION_SETTING,
            "",
        )


    def _deferred_stop_active(
        self,
        db: Session,
        *,
        batch_id: int | None = None,
        operation_id: str | None = None,
    ) -> bool:
        if not bool(repo.get_setting(db, "stop_auto_wait_external_result", False)):
            return False

        if batch_id is not None:
            try:
                pending_bid = int(repo.get_setting(db, "stop_auto_wait_batch_id", 0) or 0)
            except Exception:
                pending_bid = 0

            if pending_bid > 0 and int(batch_id) != pending_bid:
                return False

        op = str(operation_id or "").strip()
        pending_op = str(repo.get_setting(db, "stop_auto_wait_operation_id", "") or "").strip()

        if op and pending_op and op != pending_op:
            return False

        return True


    def _clear_deferred_stop(self, db: Session) -> None:
        repo.set_setting(db, "stop_auto_wait_external_result", False)
        repo.set_setting(db, "stop_auto_wait_batch_id", 0)
        repo.set_setting(db, "stop_auto_wait_operation_id", "")
        repo.set_setting(db, "stop_auto_wait_reason", "")
        repo.set_setting(db, "stop_auto_wait_r2_state", "")


    def _request_deferred_stop_after_external_result(
        self,
        db: Session,
        *,
        batch_id: int,
        operation_id: str | None,
        reason: str,
        r2_state: str,
        message: str | None = None,
        severity: str | Severity = Severity.warn,        
    ) -> None:
        repo.set_setting(db, "stop_auto_wait_external_result", True)
        repo.set_setting(db, "stop_auto_wait_batch_id", int(batch_id))
        repo.set_setting(db, "stop_auto_wait_operation_id", str(operation_id or ""))
        repo.set_setting(db, "stop_auto_wait_reason", str(reason))
        repo.set_setting(db, "stop_auto_wait_r2_state", str(r2_state))

        sev = severity.value if hasattr(severity, "value") else str(severity)
        ui_message = message or (
            f"СТОП принят: RS007L ожидает результат "
            f"операции на микрометре. "
            "После этого будет отправлен стоп на РТК и партия будет подготовлена к извлечению."
        )

        stop_message_key = (
            _batch_operator_message_key(
                STOP_MESSAGE_PREFIX,
                batch_id,
            )
            or STOP_MESSAGE_PREFIX
        )

        quality_message_key = None
        stop_ui_message = ui_message

        if str(reason) == "ppod_exceeded":
            quality_message_key = _batch_operator_message_key(
                QUALITY_PPOD_MESSAGE_PREFIX,
                batch_id,
            )

            if quality_message_key:
                _set_message(
                    db,
                    ui_message,
                    sev,
                    message_key=quality_message_key,
                )

            stop_ui_message = (
                f"СТОП партии ожидает передачи "
                f"текущего результата РТК; после выхода "
                "RS007L из ожидания будет выполнено безопасное завершение партии."
            )

        _set_message(
            db,
            stop_ui_message,
            (
                Severity.warn
                if str(reason) == "ppod_exceeded"
                else sev
            ),
            message_key=stop_message_key,
            mode=SystemMode.auto_running.value,
            active_batch_phase="stopping_wait_external_result",
        )

        event_message = (
            "PPOD deferred until R2 leaves external result wait"
            if str(reason) == "ppod_exceeded"
            else "STOP_AUTO deferred until R2 leaves external result wait"
        )

        repo.add_event(
            db,
            severity=sev,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": event_message,
                "ui_message": ui_message,
                "stop_ui_message": stop_ui_message,
                "message_key": stop_message_key,
                "quality_message_key": quality_message_key,
                "batch_id": int(batch_id),
                "operation_id": operation_id,
                "reason": str(reason),
                "r2_state": str(r2_state),
            },
        )


    def _maybe_finish_deferred_stop_after_external_result(
        self,
        db: Session,
        *,
        a2: str,
    ) -> bool:
        """
        True  - есть отложенный STOP, tick должен закончиться после обработки.
        False - отложенного STOP нет.
        """
        if not bool(repo.get_setting(db, "stop_auto_wait_external_result", False)):
            return False

        try:
            bid = int(repo.get_setting(db, "stop_auto_wait_batch_id", 0) or 0)
        except Exception:
            bid = 0

        stop_message_key = (
            _batch_operator_message_key(
                STOP_MESSAGE_PREFIX,
                bid,
            )
            or STOP_MESSAGE_PREFIX
        )

        a2n = str(a2 or "").strip().lower()

        if a2n in {"waitingmmresult", "waitingcalibrationresult"}:
            st = repo.ensure_state_row(db)
            if (st.active_batch_phase or "") != "stopping_wait_external_result":
                _set_message(
                    db,
                    (
                        f"СТОП ожидает завершения обмена с РТК: "
                        f"RS007L ещё в состоянии ожидания результата с микрометра."
                    ),
                    Severity.warn,
                    message_key=stop_message_key,
                    mode=SystemMode.auto_running.value,
                    active_batch_phase="stopping_wait_external_result",
                )
            return True

        reason = str(
            repo.get_setting(
                db,
                "stop_auto_wait_reason",
                "operator_stop_auto",
            )
            or "operator_stop_auto"
        )

        if bid <= 0:
            self._clear_deferred_stop(db)
            return False

        is_ppod_stop = reason == "ppod_exceeded"

        ok, err = self.handle_command(
            db,
            "ABORT_ACTIVE_BATCH_FOR_EXTRACTION",
            {
                "batch_id": int(bid),
                "reason": reason,
                "reject_reason": (
                    "current batch defects greater then ppod"
                    if is_ppod_stop
                    else reason
                ),
                "source": (
                    "PPOD_DEFERRED"
                    if is_ppod_stop
                    else "STOP_AUTO_DEFERRED"
                ),
                "defer_if_r2_waiting": False,                
            },
        )

        if ok:
            self._clear_deferred_stop(db)
        else:
            _set_message(
                db,
                f"Ошибка завершения отложенного STOP для партии: {err}",
                Severity.error,
                message_key=stop_message_key,
            )

        return True


    def _set_message_if_changed(
        self,
        db: Session,
        message: str,
        severity: str | Severity = Severity.info.value,
        *,
        message_key: str = "process.notice",
        **state,
    ) -> bool:
        """
        Обновляет операторское сообщение только при реальном изменении
        и публикует STATE_UPDATED, чтобы workplace сразу перечитал /state.
        """
        sev = (
            severity.value
            if hasattr(severity, "value")
            else str(severity)
        )

        current_message = next(
            (
                item
                for item in repo.list_operator_messages(db)
                if item.get("key") == message_key
            ),
            None,
        )

        message_changed = (
            current_message is None
            or current_message.get("message") != message
        )
        severity_changed = (
            current_message is None
            or current_message.get("severity") != sev
        )

        if (
            not message_changed
            and not severity_changed
            and not state
        ):
            return False

        _set_message(
            db,
            message,
            sev,
            message_key=message_key,
            **state,
        )

        event_payload = {
            "message": message,
            "message_key": message_key,
            "message_severity": sev,
        }

        if state:
            event_payload["state_fields"] = sorted(
                state.keys()
            )

        repo.add_event(
            db,
            severity=sev,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload=event_payload,
        )

        return True


    def _sync_positioner_missing_message(
        self,
        db: Session,
        *,
        snap,
        a1: str,
        a2: str,
    ) -> bool:
        """
        Синхронизирует sticky-предупреждение positioner.missing
        с фактической комбинацией сигналов РТК.

        Метод вызывается до веток с ранним return, поэтому сообщение
        не зависает и не пропадает из-за параллельной аппаратной аварии.
        """
        st = repo.ensure_state_row(db)

        positioner_part_present = getattr(
            snap,
            "positioner_part_present",
            None,
        )
        watchdog_r1 = getattr(
            snap,
            "watchdog_r1",
            None,
        )
        watchdog_r2 = getattr(
            snap,
            "watchdog_r2",
            None,
        )

        positioner_missing = (
            st.mode == SystemMode.auto_running.value
            and st.active_batch_id is not None
            and bool(getattr(snap, "connected", False))
            and str(a1 or "").strip().lower() == "waitposfree"
            and str(a2 or "").strip().lower() == "waitposfull"
            and watchdog_r1 is True
            and watchdog_r2 is True
            and positioner_part_present is False
        )

        if positioner_missing:
            msg = (
                "Отсутствует деталь на позиционере! Нажмите аварийную кнопку, войдите "
                "в ячейку и проведите осмотр!"
            )

            # publish_operator_message идемпотентен: одинаковая публикация
            # не обновляет timestamps и не пишет лишние строки в БД.
            _set_message(
                db,
                msg,
                Severity.warn,
                message_key="positioner.missing",
            )

            if not self._positioner_missing_emitted:
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": msg,
                        "message_key": "positioner.missing",
                        "where": "RTK_POSITIONER_PART_MISSING",
                        "active_batch_id": st.active_batch_id,
                        "r1_action": str(a1 or ""),
                        "r2_action": str(a2 or ""),
                        "r1_watchdog": True,
                        "r2_watchdog": True,
                        "di10": False,
                    },
                )

            self._positioner_missing_emitted = True
            return True

        repo.clear_operator_message(
            db,
            "positioner.missing",
        )
        self._positioner_missing_emitted = False
        return False


    def _sync_blocked_batch_door_messages(
        self,
        db: Session,
    ) -> None:
        """
        Синхронизирует сообщения batch.<id>.doors для всех blocked-партий.

        Для специальной детали _batch_door_cells() автоматически использует
        выгрузочную ячейку №16. При потере связи с постаматами сохранённые
        door-сообщения не удаляются: состояние датчиков в этот момент
        подтвердить невозможно.
        """
        if not self._io_connected():
            return

        try:
            pending = (
                db.execute(
                    select(BatchRow)
                    .where(BatchRow.status == "blocked")
                    .order_by(BatchRow.id.asc())
                    .limit(50)
                )
                .scalars()
                .all()
            )
        except Exception:
            return

        unresolved_keys: set[str] = set()

        for batch in pending:
            batch_id = int(batch.id)
            message_key = _batch_doors_message_key(batch_id)
            loading_cell, unloading_cell = _batch_door_cells(batch)

            if loading_cell is None or unloading_cell is None:
                unresolved_keys.add(message_key)
                self._set_message_if_changed(
                    db,
                    (
                        f"Партия не запущена: "
                        "не определены ячейки загрузки или выгрузки"
                    ),
                    Severity.error,
                    message_key=message_key,
                )
                continue

            try:
                ld_closed = self.io.read_loading_door_status(
                    int(loading_cell)
                )
            except Exception:
                ld_closed = None

            try:
                ud_closed = self.io.read_unloading_door_status(
                    int(unloading_cell)
                )
            except Exception:
                ud_closed = None

            (
                loading_seen_open,
                unloading_seen_open,
            ) = _batch_door_seen_flags(batch)

            door_data = dict(batch.data or {})
            door_flags_changed = False

            if ld_closed is False and not loading_seen_open:
                loading_seen_open = True
                door_data["loading_door_seen_open"] = True
                door_flags_changed = True

            if ud_closed is False and not unloading_seen_open:
                unloading_seen_open = True
                door_data["unloading_door_seen_open"] = True
                door_flags_changed = True

            if door_flags_changed:
                batch.data = door_data
                flag_modified(batch, "data")
                db.commit()

            if (
                loading_seen_open
                and unloading_seen_open
                and ld_closed is True
                and ud_closed is True
            ):
                repo.set_batch_status(
                    db,
                    batch_id,
                    status="loaded",
                    loaded_at=utcnow(),
                )
                repo.clear_operator_message(
                    db,
                    message_key,
                )
                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "batch_id": batch_id,
                        "status": "loaded",
                        "reason": "doors_opened_then_closed",
                        "loading_cell_no": int(loading_cell),
                        "unloading_cell_no": int(unloading_cell),
                        "message_key": message_key,
                    },
                )
                continue

            unresolved_keys.add(message_key)

            if not (loading_seen_open and unloading_seen_open):
                message = _batch_doors_open_wait_message(
                    batch_id=batch_id,
                    loading_cell=int(loading_cell),
                    unloading_cell=int(unloading_cell),
                    loading_seen_open=loading_seen_open,
                    unloading_seen_open=unloading_seen_open,
                )
            else:
                message = _batch_doors_wait_message(
                    batch_id=batch_id,
                    loading_cell=int(loading_cell),
                    unloading_cell=int(unloading_cell),
                    loading_closed=ld_closed,
                    unloading_closed=ud_closed,
                )

            self._set_message_if_changed(
                db,
                message,
                Severity.warn,
                message_key=message_key,
            )

        # Удаляем сообщения партий, которые уже не blocked либо чьи двери
        # были подтверждённо закрыты в этой итерации.
        for item in repo.list_operator_messages(db):
            key = str(item.get("key") or "")
            if (
                re.fullmatch(r"batch\.\d+\.doors", key)
                and key not in unresolved_keys
            ):
                repo.clear_operator_message(db, key)


    def _request_rejectbin_full_pause_when_safe(
        self,
        db: Session,
        *,
        reject_count: int,
        a2: str,
    ) -> bool:
        """
        Возвращает True, если tick должен остановиться после запуска paused_rejectbin.

        reject_bin_count становится полным сразу после NOK-результата ИМ,
        но физически деталь ещё не в таре брака. Поэтому:
        - сначала ставим pending;
        - ждём R2 action PutToDefect;
        - только потом запускаем сценарий выгрузки тары брака.
        """
        st = repo.ensure_state_row(db)
        a2n = str(a2 or "").strip().lower()

        if st.mode == SystemMode.paused_rejectbin.value:
            return True

        if self.reject_bin_capacity <= 0:
            return False

        if int(reject_count or 0) < int(self.reject_bin_capacity):
            had_pending_workflow = bool(
                self._rejectbin_full_pause_pending
            )
            self._rejectbin_full_pause_pending = False
            self._rejectbin_full_pause_count = 0
            self._rejectbin_full_pause_batch_id = None

            if had_pending_workflow:
                repo.clear_operator_message(
                    db,
                    REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )
            return False

        # Уже полный счётчик, но сценарий замены ещё не стартовал.
        if not self._rejectbin_full_pause_pending:
            self._rejectbin_full_pause_pending = True
            self._rejectbin_full_pause_count = int(reject_count or 0)
            self._rejectbin_full_pause_batch_id = int(st.active_batch_id) if st.active_batch_id else None

            _set_message(
                db,
                f"Тара брака заполнена ({int(reject_count)}/{int(self.reject_bin_capacity)}); ожидание останова R2",
                Severity.warn,
                message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
            )
            repo.add_event(
                db,
                severity=Severity.warn.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": "reject bin full; wait R2 pause",
                    "reject_bin_count": int(reject_count),
                    "reject_bin_capacity": int(self.reject_bin_capacity),
                    "r2_action": a2n,
                    "batch_id": self._rejectbin_full_pause_batch_id,
                },
            )

        # Главная правка: паузим только когда робот дошёл до операции укладки в брак.
        if a2n == "waitposfull":
            self._rejectbin_full_emitted = True

            # Начинается новая автоматическая замена.
            # Номер устанавливаемой тары оператор ещё не вводил.
            repo.set_setting(
                db,
                REJECT_BIN_REPLACEMENT_TARE_SETTING,
                0,
            )
            repo.set_setting(
                db,
                REJECT_BIN_REPLACEMENT_OPERATOR_SETTING,
                "",
            )

            self._begin_rejectbin_unload(
                db,
                reason="rejectbin_full",
                severity=Severity.error,
            )

            repo.add_event(
                db,
                severity=Severity.error.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": "reject bin full pause started at R2 waitposfull",
                    "reject_bin_count": int(reject_count),
                    "reject_bin_capacity": int(self.reject_bin_capacity),
                    "r2_action": a2n,
                    "batch_id": self._rejectbin_full_pause_batch_id,
                },
            )

            self._rejectbin_full_pause_pending = False
            self._rejectbin_full_pause_count = 0
            self._rejectbin_full_pause_batch_id = None
            return True

        return False


    def _begin_rejectbin_unload(
        self,
        db: Session,
        *,
        reason: str,
        severity: str | Severity = Severity.warn.value,
    ):
        """
        Старт сценария выгрузки тары брака.
        Ничего не сбрасываем здесь: только ставим систему в paused_rejectbin
        и дальше tick ждёт R2 paused, открывает дверь и ждёт закрытия.
        """
        sev = severity.value if hasattr(severity, "value") else str(severity)
        st = repo.ensure_state_row(db)

        if reason == "rejectbin_full":
            msg = "ПАУЗА: Тара брака заполнена! Ожидание остановки робота."
        else:
            msg = "ПАУЗА: Запрошена замена тары брака! Ожидание остановки робота."

        if st.mode != SystemMode.paused_rejectbin.value:
            self._pause_to(
                db,
                SystemMode.paused_rejectbin.value,
                msg,
                EventType.SYSTEM_PAUSED.value,
                sev,
                message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
            )
        else:
            self._set_message_if_changed(
                db,
                msg,
                sev,
                message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
            )

        if self._resume_target_mode == SystemMode.auto_running.value:
            if reason == "operator_request":
                self._rtk_pause(db, "rejectbin_unload")

        self._rejectbin_unload_requested = True

        self._rejectbin_door_open_requested = False
        self._rejectbin_door_open_requested_at = 0.0
        self._rejectbin_door_seen_open = False
        self._rejectbin_door_closed_since = None
        self._rejectbin_tare_seen_absent = False

        self._rejectbin_reset_done = False

        self._rejectbin_unload_reason = reason
        self._rejectbin_reopen_requested_after_missing = False

        repo.add_event(
            db,
            severity=sev,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={"rejectbin_unload": "requested", "reason": reason},
        )


    def _finish_rejectbin_unload(self, db: Session):
        """
        Финализация после того, как дверь тары брака была открыта и снова закрыта:
        - закрываем старую reject_bin запись;
        - печатаем протокол заполнения браковочной паллеты;
        - открываем новую;
        - сбрасываем локальный счетчик;
        - отправляем reset_defect_counter на РТК;
        - НЕ resume'им автоматически. Продолжение только по кнопке ПРОДОЛЖИТЬ.
        """
        if self._rejectbin_reset_done:
            return

        new_tare_no = _normalize_rejectbin_tare_no(
            repo.get_setting(
                db,
                REJECT_BIN_REPLACEMENT_TARE_SETTING,
                0,
            )
        )
        
        if new_tare_no is None:
            _set_message(
                db,
                (
                    "Невозможно завершить замену: "
                    "не указан корректный номер новой тары"
                ),
                Severity.error,
                message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
            )
            return

        rb = repo.ensure_active_reject_bin(db, self.reject_bin_capacity)
        old_tare_no = _normalize_rejectbin_tare_no(
            getattr(rb, "tare_no", None)
        )
        
        st = repo.ensure_state_row(db)
        reject_count = int(st.reject_bin_count or 0)

        repo.close_reject_bin(
            db,
            rb.id,
            count_at_close=reject_count,
            reason=self._rejectbin_unload_reason or "replaced",
        )

        try:
            db.refresh(rb)
        except Exception:
            pass

        rejectbin_protocol_path: str | None = None
        rejectbin_protocol_error: str | None = None

        try:
            protocol_text = self._rejectbin_pallet_protocol_text(
                db,
                rb,
                count_at_close=reject_count,
            )

            rejectbin_protocol_path = self.printer.print_text(
                protocol_text,
                job_name=(
                    f"rejectbin_{int(rb.id)}"
                    f"_tare_{old_tare_no if old_tare_no is not None else 'unknown'}"
                    "_protocol"
                ),
            )

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.DOCS_PRINTED.value,
                payload={
                    "doc": "rejectbin_pallet_protocol",
                    "reject_bin_id": int(rb.id),
                    "path": rejectbin_protocol_path,
                    "count_at_close": int(reject_count),
                    "printer": self.printer.__class__.__name__,
                    "spool_dir": str(getattr(self.printer, "spool_dir", "")),
                },
            )

        except Exception as e:
            rejectbin_protocol_error = str(e)

            repo.add_event(
                db,
                severity=Severity.error.value,
                source="DAEMON",
                type_=EventType.ERROR.value,
                payload={
                    "where": "PRINT_REJECTBIN_PALLET_PROTOCOL",
                    "err": rejectbin_protocol_error,
                    "reject_bin_id": int(getattr(rb, "id", 0) or 0),
                    "count_at_close": int(reject_count),
                    "printer": self.printer.__class__.__name__,
                    "spool_dir": str(getattr(self.printer, "spool_dir", "")),
                },
            )

        new_rb = repo.open_reject_bin(db, self.reject_bin_capacity)
        # open_reject_bin создаёт новую логическую тару.
        # Присваиваем ей физический номер, введённый оператором.
        new_rb.tare_no = int(new_tare_no)
        db.add(new_rb)
        db.commit()
        
        try:
            db.refresh(new_rb)
        except Exception:
            pass

        # Сбрасываем только логический счетчик в БД/state.
        repo.set_state(
            db,
            reject_bin_count=0,
            reject_bin_capacity=self.reject_bin_capacity,
        )

        # Сброс счётчика РТК должен быть поставлен в FIFO непосредственно
        # перед resume. Пока оператор не нажал ПРОДОЛЖИТЬ, робот остаётся
        # на паузе и новый брак появиться не может.
        repo.set_setting(
            db,
            REJECT_BIN_RESET_COUNTER_PENDING_SETTING,
            True,
        )
        repo.add_event(
            db,
            severity=Severity.info.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": "reset_defect_counter scheduled before resume",
                "reason": "rejectbin_replaced",
            },
        )

        if rejectbin_protocol_path:
            msg = (
                f"Установлена тара брака №{new_tare_no}; "
                "протокол снятой тары "
                "отправлен на печать; нажмите ПРОДОЛЖИТЬ"
            )
            msg_severity = Severity.warn
        elif rejectbin_protocol_error:
            msg = (
                f"Установлена тара брака №{new_tare_no}; "
                "ОШИБКА печати протокола "
                f"тары брака: {rejectbin_protocol_error}; "
                "нажмите ПРОДОЛЖИТЬ"
            )
            msg_severity = Severity.error
        else:
            msg = "Успешная замена тары брака. Нажмите ПРОДОЛЖИТЬ"
            msg_severity = Severity.warn

        _set_message(
            db,
            msg,
            msg_severity,
            message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
            reject_bin_count=0,
            reject_bin_capacity=self.reject_bin_capacity,
        )

        repo.add_event(
            db,
            severity=Severity.warn.value,
            source="DAEMON",
            type_=EventType.REJECTBIN_REPLACED.value,
            payload={
                "old_id": int(rb.id),
                "new_id": int(new_rb.id),
                "old_tare_no": old_tare_no,
                "new_tare_no": int(new_tare_no),
                "count_at_close": int(reject_count),
                "reason": self._rejectbin_unload_reason,
                "rejectbin_protocol_path": rejectbin_protocol_path,
                "rejectbin_protocol_error": rejectbin_protocol_error,
            },
        )
        repo.add_event(
            db,
            severity=Severity.info.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": msg,
                "reject_bin_count": 0,
                "rejectbin_unload": "rjb replace done, wait resume",
                "rejectbin_protocol_path": rejectbin_protocol_path,
                "rejectbin_protocol_error": rejectbin_protocol_error,
            },
        )

        self._near_full_emitted = False
        self._rejectbin_full_emitted = False

        self._rejectbin_unload_requested = False

        self._rejectbin_door_open_requested = False
        self._rejectbin_door_open_requested_at = 0.0
        self._rejectbin_door_seen_open = False
        self._rejectbin_door_closed_since = None
        self._rejectbin_tare_seen_absent = False

        self._rejectbin_reopen_requested_after_missing = False

        self._rejectbin_full_pause_pending = False
        self._rejectbin_full_pause_count = 0
        self._rejectbin_full_pause_batch_id = None

        self._rejectbin_reset_done = True
        self._rejectbin_unload_reason = None
        

    def _rtk_r2_allows_rejectbin_door(self, db: Session, a2: str) -> bool:
        """
        Дверь тары брака можно открывать:
        - если система НЕ в auto_running — не ждём R2 paused;
        - если R2 уже paused — штатный случай после pause;
        - если R2 уже waitingforstart / waitingforcommand — робот уже не полезет к таре
          до следующей партии, поэтому ждать paused бессмысленно.
        """
        st = repo.ensure_state_row(db)
        a2 = str(a2 or "").strip().lower()

        if st.mode != SystemMode.paused_rejectbin.value:
            return False

        target = self._resume_target_mode or SystemMode.idle.value

        if target != SystemMode.auto_running.value:
            return True

        return a2 in {
            "paused",
            "waitingforstart",
            "waitingforcommand",
        }


    def _rejectbin_unload_message(
        self,
        *,
        stage: str,
    ) -> str:
        if stage == "wait_r2":
            return (
                "ПАУЗА: Замена тары брака! "
                "Ожидание остановки робота."
            )

        if stage == "door_opening":
            return (
                "Открывается дверца тары брака. "
                "Дождитесь открытия."
            )

        if stage == "remove_old_tare":
            return (
                "Дверца открыта. "
                "Извлеките старую тару брака."
            )

        if stage == "install_new_tare":
            return (
                "Старая тара извлечена. "
                "Установите пустую тару брака."
            )

        if stage == "close_door":
            return (
                "Пустая тара установлена. "
                "Закройте дверцу."
            )

        if stage == "wait_resume":
            return (
                "Успешная замена тары брака. "
                "Нажмите кнопку ПРОДОЛЖИТЬ."
            )

        return "Успешная замена тары брака"


    def _request_rejectbin_door_reopen_once(
        self,
        db: Session,
        *,
        idx: int,
        reason: str,
    ) -> None:
        """
        Повторно открывает дверь только один раз
        до следующего фактического открытия.
        """
        if self._rejectbin_reopen_requested_after_missing:
            return

        self.io.request_open_loading_cell(
            int(idx)
        )

        self._rejectbin_reopen_requested_after_missing = True
        self._rejectbin_door_open_requested = True
        self._rejectbin_door_open_requested_at = float(
            self.clock()
        )
        self._rejectbin_door_seen_open = False
        self._rejectbin_door_closed_since = None

        repo.add_event(
            db,
            severity=Severity.error.value,
            source="DAEMON",
            type_=(
                EventType
                .REJECTBIN_DOOR_OPEN_REQUESTED
                .value
            ),
            payload={
                "side": "rejectbin",
                "idx": int(idx),
                "reason": str(reason),
            },
        )


    def _resolve_operation_payload(
        self,
        db: Session,
        payload: dict | None,
        *,
        where: str,
        allow_manual_without_operation: bool = True,
    ) -> tuple[bool, int | None, str | None]:
        payload = payload or {}
        st = repo.ensure_state_row(db)

        raw_bid = payload.get("batch_id")
        raw_op = payload.get("operation_id")
        op_id = str(raw_op or "").strip()

        has_payload_context = raw_bid is not None or bool(op_id)
        has_active_context = bool(
            str(getattr(st, "active_operation_id", "") or "").strip()
        )

        if not has_payload_context:
            if has_active_context and not allow_manual_without_operation:
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "operation command ignored: missing operation context",
                        "where": where,
                        "active_batch_id": getattr(st, "active_batch_id", None),
                        "active_operation_id": getattr(st, "active_operation_id", None),
                    },
                )
                return False, None, None

            # ручной/debug-сценарий вне автооперации
            bid = int(st.active_batch_id) if st.active_batch_id is not None else None
            return True, bid, None

        try:
            bid = int(raw_bid)
        except Exception:
            repo.add_event(
                db,
                severity=Severity.warn.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": "operation command ignored: invalid batch_id",
                    "where": where,
                    "payload_batch_id": raw_bid,
                    "payload_operation_id": op_id or None,
                },
            )
            return False, None, op_id or None

        if repo.active_operation_matches(
            db,
            batch_id=bid,
            operation_id=op_id,
        ):
            return True, bid, op_id

        repo.add_event(
            db,
            severity=Severity.warn.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": "stale operation command ignored",
                "where": where,
                "payload_batch_id": bid,
                "payload_operation_id": op_id or None,
                "active_batch_id": getattr(st, "active_batch_id", None),
                "active_operation_id": getattr(st, "active_operation_id", None),
                "active_operation_batch_id": getattr(st, "active_operation_batch_id", None),
            },
        )
        return False, bid, op_id or None


    def _can_open_postamat_cell(
        self,
        db: Session,
        payload: dict | None,
    ) -> tuple[bool, str | None]:
        """
        Проверка разрешения на открытие ячейки постамата.

        Обычные команды открытия сохраняют прежнюю строгую проверку
        через _can_resume().

        Извлечение уже завершённой партии (done/rejected) не является
        возобновлением автоматического цикла и не зависит от:
        - аварийного контура;
        - давления воздуха;
        - наличия тары брака;
        - температуры;
        - заполнения тары брака.

        Для такого открытия требуется только доступная связь с
        постаматами. Статус партии, extraction_ready и безопасное
        завершение R2 проверяются отдельно.
        """
        purpose = str(
            (payload or {}).get("purpose") or ""
        ).strip().lower()

        if purpose != "extraction":
            return self._can_resume(db)

        if not self._io_connected():
            return (
                False,
                self._postamat_offline_problem(
                    "Невозможно открыть ячейку для извлечения"
                )
                or "Нет связи с постаматами",
            )

        return True, None


    def _validate_extraction_open_payload(
        self,
        db: Session,
        payload: dict | None,
    ) -> tuple[bool, str | None]:
        payload = payload or {}

        if str(payload.get("purpose") or "") != "extraction":
            return True, None

        try:
            batch_id = int(payload.get("batch_id") or 0)
        except Exception:
            batch_id = 0

        if batch_id <= 0:
            return False, "payload.batch_id is required for extraction"

        batch = repo.get_batch(db, batch_id)
        if not batch:
            return False, f"batch {batch_id} not found"

        if batch.status not in {"done", "rejected"}:
            return False, (
                f"Извлечение партии запрещено: "
                f"текущий статус {batch.status}"
            )

        data = dict(batch.data or {})
        if data.get("extraction_ready") is False:
            return False, (
                f"Извлечение партии временно запрещено: "
                "роботы ещё завершают безопасную остановку"
            )

        return True, None


    def _sync_stop_cleanup_unmeasured(
        self,
        db: Session,
        *,
        batch: BatchRow,
        snap,
    ) -> tuple[bool, int, str | None]:
        if snap is None or not bool(getattr(snap, "connected", False)):
            return False, 0, "РТК не на связи"

        raw_defect_count = getattr(snap, "defectcount", None)
        if raw_defect_count is None:
            return False, 0, "РТК не передал defectcount"

        try:
            robot_defect_count = max(0, int(raw_defect_count))
        except Exception:
            return False, 0, (
                f"Некорректный defectcount РТК: {raw_defect_count!r}"
            )

        state = repo.ensure_state_row(db)
        db_reject_count = max(
            0,
            int(getattr(state, "reject_bin_count", 0) or 0),
        )

        data = dict(batch.data or {})

        # defectcount РТК является абсолютным счётчиком контроллера,
        # а reject_bin_count относится к текущей физической таре.
        # После замены тары локальный счётчик становится нулём, поэтому
        # сравнивать эти два значения напрямую нельзя: старое значение
        # defectcount было бы повторно принято за новые детали.
        #
        # Во время STOP cleanup учитываем только положительное приращение
        # defectcount относительно последнего уже обработанного snapshot.
        # Снижение счётчика означает reset; оно только переносит baseline.
        raw_previous_defect_count = data.get(
            "stop_cleanup_last_defectcount"
        )
        if raw_previous_defect_count is None:
            raw_previous_defect_count = data.get(
                "stop_cleanup_defectcount_at_start"
            )

        try:
            previous_robot_defect_count = (
                db_reject_count
                if raw_previous_defect_count is None
                else max(0, int(raw_previous_defect_count))
            )
        except Exception:
            previous_robot_defect_count = db_reject_count

        missing = max(
            0,
            robot_defect_count - previous_robot_defect_count,
        )

        for _ in range(missing):
            repo.add_reject_item(
                db,
                batch_id=int(batch.id),
                measured_params={
                    "unmeasured": True,
                    "source": "stop_cleanup",
                    "items": [],
                },
                reason="stop_unmeasured",
            )

        if missing > 0:
            # add_reject_item в штатном сценарии обновляет счётчик.
            # Явно фиксируем ожидаемое заполнение именно текущей тары.
            # Абсолютный defectcount РТК здесь использовать нельзя,
            # поскольку после замены он может ещё содержать значение
            # предыдущей тары.
            repo.set_state(
                db,
                reject_bin_count=int(db_reject_count + missing),
            )

        unmeasured_items = (
            db.execute(
                select(RejectBinItemRow).where(
                    RejectBinItemRow.batch_id == int(batch.id)
                )
            )
            .scalars()
            .all()
        )

        unmeasured_total = sum(
            1
            for item in unmeasured_items
            if (
                isinstance(item.measured_params, dict)
                and bool(item.measured_params.get("unmeasured"))
            )
        )

        data["unmeasured_after_stop_qty"] = int(
            unmeasured_total
        )
        data["stop_cleanup_last_defectcount"] = int(
            robot_defect_count
        )
        batch.data = data
        flag_modified(batch, "data")
        db.commit()

        if missing > 0:
            repo.add_event(
                db,
                severity=Severity.warn.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": (
                        f"После СТОП учтено неизмеренных деталей: "
                        f"{int(missing)}"
                    ),
                    "batch_id": int(batch.id),
                    "added": int(missing),
                    "unmeasured_total": int(unmeasured_total),
                    "rtk_defectcount": int(robot_defect_count),
                    "rtk_defectcount_before": int(
                        previous_robot_defect_count
                    ),
                    "reject_bin_count_before": int(db_reject_count),
                    "reject_bin_count_after": int(
                        db_reject_count + missing
                    ),
                },
            )

        return True, int(missing), None


    def _finish_stop_cleanup_batch(
        self,
        db: Session,
        *,
        batch: BatchRow,
        now: float,
    ) -> None:
        batch_id = int(batch.id)


        try:
            db.refresh(batch)
        except Exception:
            pass

        self._maybe_finalize_open_tare(
            db,
            batch,
            reason="stop_cleanup_finished",
        )

        try:
            db.refresh(batch)
        except Exception:
            pass

        data = dict(batch.data or {})
        abort_reason = str(
            data.get("abort_reason")
            or "operator_stop_auto"
        )
        source_cmd = str(
            data.get("abort_source")
            or "STOP_AUTO"
        )
        interrupted = data.get("abort_interrupted") or []

        data["stop_cleanup_active"] = False
        data["stop_cleanup_finished_at"] = (
            utcnow().isoformat()
        )
        data["extraction_ready"] = True
        data["extraction_wait_rtk_safe"] = False
        data["extraction_blocked_rtk_state"] = None
        batch.data = data
        flag_modified(batch, "data")

        repo.set_batch_status(
            db,
            batch_id,
            status="rejected",
            finished_at=utcnow(),
            reject_reason=abort_reason,
        )

        try:
            db.refresh(batch)
        except Exception:
            pass


        self._mm_req_sent_batch = None
        self._calib_req_sent_batch = None
        self._calib_program_load_req_key = None

        self._calib_due_at = None
        self._calib_due_batch = None
        self._calib_due_operation_id = None
        self._calib_due_reject_threshold = None

        self._reset_tare_tracking(
            reason="stop_cleanup_finished",
            batch_id=batch_id,
        )

        if (
            self._ppod_tripped_batch_id is not None
            and int(self._ppod_tripped_batch_id) == batch_id
        ):
            self._clear_ppod_latch(
                db,
                reason=abort_reason,
            )

        self._queue_sound(now, 2)
        self._auto_start_block_until = max(
            float(
                getattr(
                    self,
                    "_auto_start_block_until",
                    0.0,
                )
            ),
            float(self._sound_hold_until),
        )

        is_ppod_stop = (
            abort_reason == "ppod_exceeded"
            or bool(data.get("ppod_exceeded"))
        )
        ppod_bad = int(data.get("ppod_measured_bad") or 0)
        ppod_limit = int(data.get("ppod_limit") or 0)
        ppod_expected = int(data.get("ppod_expected") or 0)
        ppod_settings = float(data.get("ppod_settings") or 0.0)

        quality_msg = None
        if is_ppod_stop:
            quality_msg = (
                f"Партия отклонена: превышен "
                "допустимый процент брака"
            )
            if ppod_expected > 0:
                quality_msg += (
                    f" ({ppod_bad} из {ppod_expected} шт., "
                    f"предел {ppod_limit})"
                )
            quality_msg += "; роботы безопасно завершили операции"

        stop_msg = (
            f"Партия безопасно завершена и "
            "подготовлена к извлечению"
        )
        stop_severity = Severity.warn

        unmeasured_total = int(
            data.get("unmeasured_after_stop_qty") or 0
        )
        if unmeasured_total > 0:
            stop_msg += (
                f"; не измерено после СТОП: "
                f"{unmeasured_total} шт."
            )
            if quality_msg is not None:
                quality_msg += (
                    f"; не измерено после СТОП: "
                    f"{unmeasured_total} шт."
                )

        if interrupted:
            stop_msg += (
                "; прервана операция: "
                + ", ".join(str(x) for x in interrupted)
            )

        stop_msg += (
            "; при наличии следующей партии "
            "автоцикл продолжится автоматически"
        )

        if quality_msg is not None:
            _set_message(
                db,
                quality_msg,
                Severity.error,
                message_key=(
                    _batch_operator_message_key(
                        QUALITY_PPOD_MESSAGE_PREFIX,
                        batch_id,
                    )
                    or QUALITY_PPOD_MESSAGE_PREFIX
                ),
            )

        _set_message(
            db,
            stop_msg,
            stop_severity,
            message_key=(
                _batch_operator_message_key(
                    STOP_MESSAGE_PREFIX,
                    batch_id,
                )
                or STOP_MESSAGE_PREFIX
            ),
            mode=SystemMode.auto_running.value,
            active_batch_id=None,
            active_batch_phase=None,
            active_batch_expected_count=None,
            active_operation_id=None,
            active_operation_batch_id=None,
            active_operation_phase=None,
            active_operation_started_at=None,
            pending_mm_result=None,
            mm_result_inflight=0,
            pending_calib_result=None,
            calib_result_inflight=0,
            calibration_inflight=0,
            calibration_reason=None,
            calib_wait_im_program=0,
            calib_im_program_target=None,
            consecutive_rejects=0,
            rtk_pickcount_seen=None,
            rtk_defectcount_seen=None,
            rtk_consecutive_defects=0,
        )

        self._resume_target_mode = SystemMode.auto_running.value
        self._clear_deferred_stop(db)

        if is_ppod_stop:
            repo.add_event(
                db,
                severity=Severity.error.value,
                source="DAEMON",
                type_=EventType.AUTO_STOPPED.value,
                payload={
                    "reason": "ppod_exceeded",
                    "batch_id": batch_id,
                    "measured_bad": ppod_bad,
                    "limit": ppod_limit,
                    "expected": ppod_expected,
                    "settings_ppod": ppod_settings,
                    "safe_stop_finished": True,
                    "mode": SystemMode.auto_running.value,
                    "auto_cycle_continues": True,
                },
            )

        repo.add_event(
            db,
            severity=(
                Severity.error.value
                if is_ppod_stop
                else Severity.warn.value
            ),
            source="DAEMON",
            type_=EventType.BATCH_REJECTED.value,
            payload={
                "batch_id": batch_id,
                "reason": abort_reason,
                "source_cmd": source_cmd,
                "unmeasured_after_stop_qty": unmeasured_total,
                "extraction_ready": True,
                "ppod_exceeded": bool(is_ppod_stop),
                "measured_bad": ppod_bad if is_ppod_stop else None,
                "limit": ppod_limit if is_ppod_stop else None,
                "expected": ppod_expected if is_ppod_stop else None,
                "settings_ppod": ppod_settings if is_ppod_stop else None,                
            },
        )

        repo.add_event(
            db,
            severity=Severity.info.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": stop_msg,
                "message_key": _batch_operator_message_key(
                    STOP_MESSAGE_PREFIX,
                    batch_id,
                ),
                "quality_message": quality_msg,
                "active_batch_id": None,
                "active_operation_id": None,
                "mode": SystemMode.auto_running.value,
                "source_cmd": source_cmd,
            },
        )



    def _cell_open_blocked_by_rtk_waiting_stop(
        self,
        db: Session,
        *,
        idx: int,
        side: str,
    ) -> tuple[bool, str | None]:
        r2_state = self._rtk_r2_external_wait_state()
        if not r2_state:
            return False, None

        batches = db.execute(
            select(BatchRow).where(
                BatchRow.status == "rejected"
            )
        ).scalars().all()

        for b in batches:
            d = dict(b.data or {})
            if not bool(d.get("extraction_wait_rtk_safe")):
                continue

            loc = dict(b.location or {})

            raw_ids = []
            if side == "loading":
                raw_ids = loc.get("in_tare_ids") or d.get("in_tare_ids") or []
            elif side == "unloading":
                raw_ids = loc.get("out_tare_ids") or d.get("out_tare_ids") or []

            if not isinstance(raw_ids, list):
                raw_ids = [raw_ids]

            ids = []
            for x in raw_ids:
                try:
                    ids.append(int(x))
                except Exception:
                    pass

            if int(idx) in ids:
                return (
                    True,
                    (
                        f"Извлечение партии временно запрещено: "
                        f"R2 ещё находится в состоянии {r2_state}. "
                        "Дождитесь фактической остановки РТК и повторите извлечение."
                    ),
                )

        return False, None


    def _check_batch_extraction_doors(
        self,
        db: Session,
        *,
        batch_id: int,
    ) -> tuple[bool, str | None]:
        """
        Подтверждает фактическое закрытие всех дверей партии
        перед печатью/освобождением ячеек.

        Проверка не зависит от safety, давления воздуха, температуры
        и режима системы: это локальная проверка датчиков дверей.
        При отсутствии связи или недостоверном значении работаем
        fail-safe и не разрешаем завершить извлечение.
        """
        bid = int(batch_id or 0)
        if bid <= 0:
            return False, "payload.batch_id is required"

        batch = repo.get_batch(db, bid)
        if not batch:
            return False, f"batch {bid} not found"

        if batch.status not in {"done", "rejected"}:
            return False, (
                f"Извлечение партии запрещено: "
                f"текущий статус {batch.status}"
            )

        data = dict(batch.data or {})
        if data.get("extraction_ready") is False:
            return False, (
                f"Извлечение партии временно запрещено: "
                "роботы ещё завершают безопасную остановку"
            )

        if not self._io_connected():
            return False, (
                self._postamat_offline_problem(
                    "Невозможно проверить закрытие ячеек"
                )
                or "Нет связи с постаматами"
            )

        loading_ids = _batch_side_cell_ids(
            batch,
            side="loading",
        )
        unloading_ids = _batch_side_cell_ids(
            batch,
            side="unloading",
        )

        if not loading_ids and not unloading_ids:
            return False, (
                f"Для партии не определены ячейки "
                "загрузки и выгрузки"
            )

        open_doors: list[str] = []
        unknown_doors: list[str] = []

        for idx in loading_ids:
            label = f"загрузки №{int(idx)}"
            try:
                closed = self.io.read_loading_door_status(
                    int(idx)
                )
            except Exception:
                closed = None

            if closed is False:
                open_doors.append(label)
            elif closed is not True:
                unknown_doors.append(label)

        for idx in unloading_ids:
            label = f"выгрузки №{int(idx)}"
            try:
                closed = self.io.read_unloading_door_status(
                    int(idx)
                )
            except Exception:
                closed = None

            if closed is False:
                open_doors.append(label)
            elif closed is not True:
                unknown_doors.append(label)

        messages: list[str] = []

        if open_doors:
            messages.append(
                "закройте двери ячеек "
                + ", ".join(open_doors)
            )

        if unknown_doors:
            messages.append(
                "не удалось подтвердить состояние дверей ячеек "
                + ", ".join(unknown_doors)
            )

        if messages:
            return False, (
                f"Партия: "
                + "; ".join(messages)
            )

        return True, None


    # ---------- commands ----------

    def handle_command(
        self,
        db: Session,
        cmd_type: str,
        payload: dict,
        *,
        created_by: str | None = None,
    ):

        if cmd_type == CommandType.PING.value:
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.PONG.value,
                payload={"ok": True},
            )
            return True, None

        if cmd_type == CommandType.SET_EMERGENCY_SOUND_MUTED.value:
            muted = bool((payload or {}).get("muted", False))
        
            repo.set_setting(
                db,
                EMERGENCY_SOUND_MUTED_SETTING,
                muted,
            )
        
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": (
                        "emergency sound muted"
                        if muted
                        else "emergency sound enabled"
                    ),
                    "emergency_sound_muted": muted,
                    "emergency_active": bool(
                        repo.get_setting(
                            db,
                            EMERGENCY_ACTIVE_SETTING,
                            False,
                        )
                    ),
                },
            )
        
            return True, None

        if cmd_type == CommandType.START_AUTO.value:
            st_start = repo.ensure_state_row(db)

            # Повторный СТАРТ во время уже работающего автоцикла не должен:
            # - перетирать операторское сообщение;
            # - повторно включать вакуум;
            # - создавать новый AUTO_STARTED.
            if (
                st_start.mode
                == SystemMode.auto_running.value
            ):
                return True, None

            ok, err = self._equipment_ready_for_auto_start(db)
            if not ok:
                # Физические причины уже отображаются независимыми
                # safety/air/equipment/temperature ключами. Ошибка самой
                # команды будет опубликована main.py как command.error.
                return False, err

            temperature_state_start = self._read_temperature_state()
            self._update_temperature_events(
                db,
                temperature_state_start,
                where="START_AUTO",
                publish_near_message=False,
                force=True,
            )

            ok, err = self._can_resume(db)
            if not ok:
                return False, err

            self._last_air_pressure_ok = True
            self._air_pressure_emergency_active = False
            self._air_pressure_lost_since = None            

            rtk_manual = bool(
                repo.get_setting(db, "rtk_manual_mode", False)
            )

            start_message = (
                "Цикл запущен: РТК работает в ручном пошаговом режиме"
                if rtk_manual
                else "Цикл запущен: РТК работает в автоматическом режиме"
            )

            start_severity = Severity.info

            if (
                str(
                    temperature_state_start.get("overall_status")
                    or ""
                )
                == "near_critical"
            ):
                start_message = (
                    f"{start_message}. "
                    + self._temperature_message(
                        temperature_state_start,
                        "near_critical",
                    )
                )
                start_severity = Severity.warn

            # Каждый новый запуск автоматического режима начинается
            # без отложенного останова от предыдущего цикла. При рестарте
            # daemon во время активной партии эта ветка не выполняется.
            repo.set_setting(
                db,
                STOP_AFTER_BATCH_SETTING,
                0,
            )

            _set_transient_message(
                db,
                start_message,
                start_severity,
                message_key="notice.system",
                mode=SystemMode.auto_running.value,
            )
            
            self._resume_target_mode = SystemMode.auto_running.value
            _enqueue_cmd(
                db,
                CommandType.IM_VACUUM_ON.value,
                {
                    "phase": "start_auto",
                    "reason": "operator_start_auto",
                },
            )
            
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.AUTO_STARTED.value,
                payload={},
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={"mode": SystemMode.auto_running.value},
            )
            return True, None

        if cmd_type == CommandType.STOP_AUTO.value:
            st = repo.ensure_state_row(db)
            reason = str((payload or {}).get("reason") or "operator_stop_auto")

            active_bid = getattr(st, "active_operation_batch_id", None)
            if active_bid is None:
                active_bid = st.active_batch_id

            active_batch = (
                repo.get_batch(db, int(active_bid))
                if active_bid is not None
                else None
            )

            # Если есть активная auto_processing партия,
            # STOP работает через процедуру подготовки партии к извлечению.
            # Внутри waitingmmresult/waitingcalibrationresult процедура будет отложена.
            if active_batch is not None and active_batch.status == "auto_processing":
                return self.handle_command(
                    db,
                    "ABORT_ACTIVE_BATCH_FOR_EXTRACTION",
                    {
                        "batch_id": int(active_batch.id),
                        "reason": reason,
                        "source": "STOP_AUTO",
                        "defer_if_r2_waiting": True,
                    },
                    created_by=created_by,
                )

            _enqueue_cmd(
                db,
                CommandType.IM_VACUUM_OFF.value,
                {
                    "phase": "stop_auto",
                    "reason": str(reason),
                },
            )

            self._clear_deferred_stop(db)

            # Если активной партии уже нет, STOP_AUTO означает переход
            # в idle (в том числе reason=queue_empty). Одноразовый запрос
            # останова после партии также считаем выполненным/отменённым.
            repo.set_setting(
                db,
                STOP_AFTER_BATCH_SETTING,
                0,
            )

            # Итоговые сообщения предыдущих партий больше не относятся
            # к текущей работе.
            _clear_completed_batch_operator_messages(db)

            _set_transient_message(
                db,
                "Автоматический режим остановлен",
                Severity.info,
                message_key="notice.system",
                mode=SystemMode.idle.value,
                active_batch_id=None,
                active_batch_phase=None,
                active_batch_expected_count=None,
                active_operation_id=None,
                active_operation_batch_id=None,
                active_operation_phase=None,
                active_operation_started_at=None,
                pending_mm_result=None,
                mm_result_inflight=0,
                pending_calib_result=None,
                calib_result_inflight=0,
                calibration_inflight=0,
                calibration_reason=None,
                calib_wait_im_program=0,
                calib_im_program_target=None,
                rtk_pickcount_seen=None,
                rtk_defectcount_seen=None,
                rtk_consecutive_defects=0,
            )

            self._resume_target_mode = None

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.AUTO_STOPPED.value,
                payload={"reason": reason},
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={"mode": SystemMode.idle.value},
            )
            return True, None

        if cmd_type == CommandType.PAUSE_SYSTEM.value:
            st = repo.ensure_state_row(db)

            # PAUSE должна быть идемпотентной. Повторная команда во время
            # любой уже активной паузы не должна менять тип паузы и,
            # главное, не должна перетирать сохранённый режим возврата.
            # Иначе двойное нажатие сохраняло paused_operator как target,
            # и последующий RESUME оставлял систему в состоянии паузы.
            if str(st.mode or "").startswith("paused"):
                return True, None

            self._resume_target_mode = st.mode

            # если автопроцесс бежит - паузим RTK (с возможностью resume)
            if st.mode == SystemMode.auto_running.value:
                self._rtk_pause(db, "operator")

            _set_transient_message(
                db,
                "Система поставлена на паузу оператором",
                Severity.warn,
                message_key="notice.system",
                mode=SystemMode.paused_operator.value,
            )
            repo.add_event(
                db,
                severity=Severity.warn.value,
                source="DAEMON",
                type_=EventType.SYSTEM_PAUSED.value,
                payload={"by": created_by},
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={"mode": SystemMode.paused_operator.value},
            )
            return True, None

        if cmd_type == CommandType.RESUME_SYSTEM.value:
            st_resume = repo.ensure_state_row(db)
            resume_after_rejectbin = (
                st_resume.mode
                == SystemMode.paused_rejectbin.value
            )
            # После результата -1 повтор операции ИМ разрешаем только после
            # реального прохождения оператором аварийного контура. Gate
            # привязан к конкретной партии и operation_id и не зависит от
            # временного локального значения rtk_paused.
            calibration_retry_safety_pending = (
                self._calibration_retry_safety_gate_matches(
                    db,
                    batch_id=(
                        getattr(
                            st_resume,
                            "active_operation_batch_id",
                            None,
                        )
                        or st_resume.active_batch_id
                    ),
                    operation_id=getattr(
                        st_resume,
                        "active_operation_id",
                        None,
                    ),
                )
            )

            if (
                calibration_retry_safety_pending
                and st_resume.mode != SystemMode.paused_safety.value
            ):
                return (
                    False,
                    (
                        "После результата -1 сначала нажмите аварийную кнопку, "
                        "проведите осмотр и восстановите аварийный контур"
                    ),
                )

            # ПРОДОЛЖИТЬ вне любого paused-режима является повторной
            # идемпотентной командой. Она не должна менять mode и сбрасывать
            # сохранённую цель возврата: именно такой повтор во время
            # auto_running ранее переводил активную партию в idle.
            if not str(st_resume.mode or "").startswith("paused"):
                return True, None

            ok, err = self._can_resume(db)
            if not ok:
                return False, err

            for message_key in PAUSE_RECOVERY_MESSAGE_KEYS:
                repo.clear_operator_message(db, message_key)

            target = self._resume_target_mode
            if not target:
                # Fail-safe после рестарта или утраты локального target:
                # активная партия/операция всегда возобновляется в auto,
                # а не переводится в idle.
                active_auto_context = (
                    st_resume.active_batch_id is not None
                    or getattr(
                        st_resume,
                        "active_operation_batch_id",
                        None,
                    ) is not None
                )
                target = (
                    SystemMode.auto_running.value
                    if active_auto_context
                    else SystemMode.idle.value
                )

            if calibration_retry_safety_pending:
                # _can_resume() уже подтвердил восстановленный safety_ok.
                self._clear_calibration_retry_safety_gate(db)

            # После замены тары reset должен уйти строго перед resume,
            # чтобы РТК продолжил работу уже с нулевым defect counter.
            if (
                resume_after_rejectbin
                and bool(
                    repo.get_setting(
                        db,
                        REJECT_BIN_RESET_COUNTER_PENDING_SETTING,
                        False,
                    )
                )
            ):
                self.rtk.request("reset_defect_counter", {})
                repo.set_setting(
                    db,
                    REJECT_BIN_RESET_COUNTER_PENDING_SETTING,
                    False,
                )
                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.RTK_COMMAND_SENT.value,
                    payload={
                        "cmd": "reset_defect_counter",
                        "reason": "rejectbin_replaced_before_resume",
                    },
                )

            # Если во время замены полной тары уже был запланирован критерий
            # N браков подряд, сначала отправляем RTK calibrate.
            # Это не даёт роботу после resume взять следующую деталь раньше проверки.
            calib_sent_on_resume = False
            if (
                target == SystemMode.auto_running.value
                and self._calib_due_at is not None
                and self._calib_due_batch is not None
            ):
                st_cal = repo.ensure_state_row(db)
                if (
                    st_cal.active_batch_id is not None
                    and int(st_cal.active_batch_id) == int(self._calib_due_batch)
                    and not bool(getattr(st_cal, "calibration_inflight", 0))
                    and str(getattr(st_cal, "active_operation_id", "") or "") == str(self._calib_due_operation_id or "")                    
                ):
                    reject_threshold = int(
                        self._calib_due_reject_threshold
                        or self._consecutive_rejects_threshold
                    )
                    etalon_id = int(self._calib_due_etalon_id)
                    self.rtk.request("calibrate", {"etalon_id": etalon_id})
                    repo.set_state(
                        db,
                        calibration_inflight=0,
                        calibration_reason="three_consecutive_rejects_im",
                        consecutive_rejects=0,
                        rtk_consecutive_defects=0,
                    )
                    _set_message(
                        db,
                        f"Критерий брака: достигнут порог подряд идущих "
                        f"браков ({reject_threshold}); запрошена проверка",
                        Severity.warn,
                        message_key=(
                            _batch_operator_message_key(
                                QUALITY_CALIBRATION_MESSAGE_PREFIX,
                                st_cal.active_batch_id,
                            )
                            or QUALITY_CALIBRATION_MESSAGE_PREFIX
                        ),
                        mode=target,
                    )
                    repo.add_event(
                        db,
                        severity=Severity.warn.value,
                        source="DAEMON",
                        type_=EventType.CALIBRATION_REQUESTED.value,
                        payload={
                            "reason": "three_consecutive_rejects_im",
                            "etalon_id": etalon_id,
                            "batch_id": int(st_cal.active_batch_id),
                            "where": "RESUME_AFTER_REJECTBIN_FULL",
                            "consecutive_rejects_threshold": reject_threshold,
                        },
                    )
                    calib_sent_on_resume = True
                    self._calib_due_at = None
                    self._calib_due_batch = None
                    self._calib_due_operation_id = None                    
                    self._calib_due_reject_threshold = None

            if not calib_sent_on_resume:
                _set_transient_message(
                    db,
                    "Работа программы возобновлена",
                    Severity.info,
                    message_key="notice.system",
                    mode=target,
                )

            if target == SystemMode.auto_running.value:
                self._rtk_resume(db)

            if resume_after_rejectbin:
                repo.clear_operator_message(
                    db,
                    REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )

            # target относится только к одной завершённой паузе. После
            # успешного RESUME не оставляем его висеть в памяти supervisor.
            self._resume_target_mode = None

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.SYSTEM_RESUMED.value,
                payload={"mode": target, "by": created_by},
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={"mode": target},
            )
            return True, None

        if cmd_type == CommandType.REPLACE_REJECTBIN.value:
            idx = 16
            st = repo.ensure_state_row(db)
            payload = payload or {}
        
            new_tare_no = _normalize_rejectbin_tare_no(
                payload.get("tare_no")
            )
        
            if new_tare_no is None:
                error_message = (
                    "Номер тары должен быть целым числом "
                    f"от {REJECT_BIN_TARE_MIN} "
                    f"до {REJECT_BIN_TARE_MAX}"
                )

                self._set_message_if_changed(
                    db,
                    error_message,
                    Severity.error,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )

                return (
                    False,
                    error_message,
                )

            # Автоматическая замена уже могла начаться из-за полного заполнения.
            # В этом случае не запускаем сценарий повторно, а только принимаем
            # номер устанавливаемой тары.
            if (
                st.mode == SystemMode.paused_rejectbin.value
                and not self._rejectbin_reset_done
            ):
                repo.set_setting(
                    db,
                    REJECT_BIN_REPLACEMENT_TARE_SETTING,
                    int(new_tare_no),
                )
                repo.set_setting(
                    db,
                    REJECT_BIN_REPLACEMENT_OPERATOR_SETTING,
                    str(created_by or "-").strip() or "-",
                )
        
                _set_message(
                    db,
                    (
                        f"Номер тары №{new_tare_no} принят. "
                        "Установите тару и закройте дверцу."
                    ),
                    Severity.warn,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )
        
                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "reject bin replacement tare number accepted",
                        "new_tare_no": int(new_tare_no),
                        "reason": self._rejectbin_unload_reason,
                    },
                )
        
                return True, None
        
            # Если замена уже полностью завершена и система ждёт ПРОДОЛЖИТЬ.
            if (
                st.mode == SystemMode.paused_rejectbin.value
                and self._rejectbin_reset_done
            ):
                _set_message(
                    db,
                    "Тара брака уже заменена. Нажмите кнопку ПРОДОЛЖИТЬ.",
                    Severity.warn,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )
                return True, None
        
            door_closed = self.io.read_loading_door_status(int(idx))
        
            if door_closed is False:
                error_message = (
                    "Дверца открыта, для продолжения "
                    "необходимо её закрыть!"
                )

                self._set_message_if_changed(
                    db,
                    error_message,
                    Severity.error,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )

                return (
                    False,
                    error_message,
                )
        
            # Номер относится к таре, которая будет установлена после извлечения.
            repo.set_setting(
                db,
                REJECT_BIN_REPLACEMENT_TARE_SETTING,
                int(new_tare_no),
            )
            repo.set_setting(
                db,
                REJECT_BIN_REPLACEMENT_OPERATOR_SETTING,
                str(created_by or "-").strip() or "-",
            )
        
            self._rejectbin_reset_done = False
            self._rejectbin_unload_requested = False

            self._rejectbin_door_open_requested = False
            self._rejectbin_door_open_requested_at = 0.0
            self._rejectbin_door_seen_open = False
            self._rejectbin_door_closed_since = None
            self._rejectbin_tare_seen_absent = False

            self._rejectbin_unload_reason = None
            self._rejectbin_reopen_requested_after_missing = False
        
            self._begin_rejectbin_unload(
                db,
                reason="operator_request",
                severity=Severity.warn,
            )
        
            return True, None

        if cmd_type == CommandType.RTK_CYCLE_ON.value:
            snap = self.rtk.snapshot()

            c1 = getattr(snap, "connected_r1", None)
            c2 = getattr(snap, "connected_r2", None)
            if c1 is None:
                c1 = snap.connected
            if c2 is None:
                c2 = snap.connected

            cs1 = getattr(snap, "cs_r1", None)
            cs2 = getattr(snap, "cs_r2", None)

            r1_reasons, r1_missing, r1_diag = (
                self._rtk_cycle_on_robot_diagnostics(
                    snap,
                    suffix="r1",
                    label="R1 (RS013N)",
                )
            )
            r2_reasons, r2_missing, r2_diag = (
                self._rtk_cycle_on_robot_diagnostics(
                    snap,
                    suffix="r2",
                    label="R2 (RS007L)",
                )
            )

            event_payload = {
                "cmd": "cycle_on",
                "connected_r1": bool(c1),
                "connected_r2": bool(c2),
                "r1": {
                    **r1_diag,
                    "missing": list(r1_missing),
                },
                "r2": {
                    **r2_diag,
                    "missing": list(r2_missing),
                },
            }

            reasons: list[str] = []
            diagnostic_warnings: list[str] = []

            if not c1:
                reasons.append("Нет связи с роботом R1 (RS013N)")
            elif cs1 is not True:
                reasons.extend(r1_reasons)
                if r1_missing:
                    diagnostic_warnings.append(
                        "R1 (RS013N): не получены флаги "
                        + ", ".join(r1_missing)
                    )

            if not c2:
                reasons.append("Нет связи с роботом R2 (RS007L)")
            elif cs2 is not True:
                reasons.extend(r2_reasons)
                if r2_missing:
                    diagnostic_warnings.append(
                        "R2 (RS007L): не получены флаги "
                        + ", ".join(r2_missing)
                    )

            # При корректных диагностических флагах cs=False означает,
            # что роботу можно отправлять cycle_on. Любая найденная причина
            # должна быть показана оператору до отправки команды.
            if reasons:
                msg = (
                    "Цикл на РТК не запущен: "
                    + "; ".join(reasons)
                )

                if diagnostic_warnings:
                    msg += (
                        "; диагностика неполна: "
                        + "; ".join(diagnostic_warnings)
                    )

                _set_message(
                    db,
                    msg,
                    Severity.error,
                    message_key="command.error",
                )

                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        **event_payload,
                        "message": msg,
                        "reasons": list(reasons),
                        "diagnostic_warnings": list(
                            diagnostic_warnings
                        ),
                    },
                )
                return False, msg

            if cs1 is True and cs2 is True:
                msg = (
                    "Цикл на РТК уже запущен"
                )

                _set_transient_message(
                    db,
                    msg,
                    Severity.warn,
                    message_key="notice.rtk",
                )

                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        **event_payload,
                        "message": msg,
                        "idempotent": True,
                    },
                )

                # Желаемое состояние уже достигнуто. Это не ошибка команды:
                # возвращаем success, чтобы main.py не создавал второе
                # одинаковое красное сообщение command.error. Успешный
                # повтор также снимет ранее зависший command.error этого типа.
                return True, None

            need_cycle_on = bool(
                c1
                and c2
                and (
                    (cs1 is False)
                    or (cs2 is False)
                )
            )

            if not need_cycle_on:
                msg = (
                    "Цикл на РТК не запущен: "
                    f"неопределённое состояние cs "
                    f"(R1={cs1}, R2={cs2})"
                )

                _set_message(
                    db,
                    msg,
                    Severity.error,
                    message_key="command.error",
                )

                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        **event_payload,
                        "message": msg,
                    },
                )
                return False, msg

            self.rtk.request("cycle_on", {})

            if diagnostic_warnings:
                success_message = (
                    "Команда запуска цикла на РТК отправлена; "
                    "диагностика неполна: "
                    + "; ".join(diagnostic_warnings)
                )
                success_severity = Severity.warn
            else:
                success_message = (
                    "Успешно запущен цикл на РТК"
                )
                success_severity = Severity.info

            _set_transient_message(
                db,
                success_message,
                success_severity,
                message_key="notice.rtk",
            )
            repo.add_event(
                db,
                severity=success_severity.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={
                    **event_payload,
                    "by": created_by,
                    "message": success_message,
                    "diagnostic_warnings": list(
                        diagnostic_warnings
                    ),
                },
            )
            return True, None


        if cmd_type == CommandType.RTK_SET_STEP_MODE.value:
            snap = self.rtk.snapshot()

            if not getattr(snap, "connected", False):
                return False, "РТК не подключён"

            raw_manual = (payload or {}).get("manual")

            if not isinstance(raw_manual, bool):
                return False, "payload.manual должен иметь тип bool"

            manual = bool(raw_manual)

            self.rtk.request(
                "set_step_mode",
                {
                    "manual": manual,
                },
            )

            # Храним выбранный режим отдельно от SystemMode.
            # SystemMode продолжает описывать состояние общего цикла.
            repo.set_setting(db, "rtk_manual_mode", manual)

            mode_name = "ручной" if manual else "автоматический"

            _set_transient_message(
                db,
                f"РТК: выбран {mode_name} режим управления",
                Severity.info,
                message_key="notice.rtk",
            )

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={
                    "cmd": "set_step_mode",
                    "manual": manual,
                    "rtk_mode": not manual,
                    "by": created_by,
                },
            )

            return True, None


        if cmd_type == CommandType.RTK_NEXT_STEP.value:
            snap = self.rtk.snapshot()

            if not getattr(snap, "connected", False):
                return False, "РТК не подключён"

            if not self._air_pressure_ok_from_snapshot(snap):
                return False, "нет давления воздуха"

            manual = bool(
                repo.get_setting(db, "rtk_manual_mode", False)
            )

            if not manual:
                return False, (
                    "Следующий шаг доступен только "
                    "в ручном режиме РТК"
                )

            st = repo.ensure_state_row(db)

            if st.mode != SystemMode.auto_running.value:
                return False, (
                    "Для выполнения следующего шага "
                    "сначала запустите цикл"
                )

            self.rtk.request("next_step", {})

            _set_transient_message(
                db,
                "РТК: отправлена команда выполнения следующего шага",
                Severity.info,
                message_key="notice.rtk",
            )

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={
                    "cmd": "next_step",
                    "by": created_by,
                    "active_batch_id": st.active_batch_id,
                },
            )

            return True, None


        if cmd_type == CommandType.RTK_PAUSE.value:
            self._rtk_pause(db, "manual")
            return True, None
        
        if cmd_type == CommandType.RTK_RESUME.value:
            self._rtk_resume(db)
            return True, None
        
        if cmd_type == CommandType.RTK_STOP.value:
            self._rtk_stop(db, "manual_stop")
            return True, None
        
        if cmd_type == CommandType.RTK_RESET.value:
            self.rtk.request("reset", payload)
            repo.add_event(
                db,
                severity=Severity.warn.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={"cmd":"reset"},
            )
            return True, None
        
        if cmd_type == CommandType.RTK_CALIBRATE.value:
            self.rtk.request("calibrate", payload)
            repo.add_event(
                db,
                severity=Severity.warn.value, 
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={"cmd":"calibrate"},
            )
            return True, None

        
        if cmd_type == CommandType.RTK_START.value:
            ok, err = self._equipment_ready_for_auto_start(db)
            if not ok:
                return False, err

            ok, err = self._can_resume(db)
            if not ok:
                return False, err

            self._last_air_pressure_ok = True
            self._air_pressure_emergency_active = False
            self._air_pressure_lost_since = None            

            snap = self.rtk.snapshot()
            if not getattr(snap, "connected", False):
                return False, "rtk not connected"

            st = repo.ensure_state_row(db)
            payload = payload or {}

            def _as_int_list(v) -> list[int]:
                if v is None:
                    return []
                items = v if isinstance(v, list) else [v]
                out: list[int] = []
                for x in items:
                    try:
                        out.append(int(x))
                    except Exception:
                        pass
                return out

            bid = payload.get("batch_id") or st.active_batch_id
            operation_id = str(payload.get("operation_id") or "").strip()

            if bid and operation_id:
                if not repo.active_operation_matches(
                    db,
                    batch_id=int(bid),
                    operation_id=operation_id,
                ):
                    repo.add_event(
                        db,
                        severity=Severity.warn.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={
                            "message": "stale RTK_START ignored",
                            "batch_id": int(bid),
                            "operation_id": operation_id,
                            "active_batch_id": getattr(st, "active_batch_id", None),
                            "active_operation_id": getattr(st, "active_operation_id", None),
                        },
                    )
                    return True, None

            elif bid and getattr(st, "active_operation_id", None):
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "RTK_START ignored: missing operation_id",
                        "batch_id": int(bid),
                        "active_operation_id": getattr(st, "active_operation_id", None),
                    },
                )
                return True, None

            b = repo.get_batch(db, int(bid)) if bid else None
            data = dict((b.data or {}) if b else {})
            loc = dict((b.location or {}) if b else {})

            product_code = (
                payload.get("ProductName")
                or payload.get("product_name")
                or payload.get("product_code")
                or data.get("product_code")
                or data.get("product_name")
            )
            product_spec = int(
                payload.get("ProductSpec")
                or payload.get("product_spec")
                or data.get("product_spec")
                or 0
            )
            product_count = int(
                payload.get("ProductCount")
                or payload.get("product_count")
                or data.get("product_count")
                or data.get("qty")
                or 0
            )

            raw_layout = (
                payload["Layout"]
                if "Layout" in payload
                else payload["layout"]
                if "layout" in payload
                else data["layout"]
                if "layout" in data
                else repo.get_setting(
                    db,
                    "layout",
                    DEFAULT_BATCH_LAYOUT,
                )
            )

            try:
                layout = int(raw_layout)
            except (TypeError, ValueError):
                return False, "Layout must be int: 0, 1, 2 or 3"

            if layout not in (0, 1, 2, 3):
                return False, "Layout must be 0, 1, 2 or 3"

            raw_use_alternate_wave = (
                payload["UseAlternateWave"]
                if "UseAlternateWave" in payload
                else payload["use_alternate_wave"]
                if "use_alternate_wave" in payload
                else data["use_alternate_wave"]
                if "use_alternate_wave" in data
                else self._setting_bool(
                    db,
                    "use_alternate_wave",
                    DEFAULT_USE_ALTERNATE_WAVE,
                )
            )

            if not isinstance(raw_use_alternate_wave, bool):
                return False, ("UseAlternateWave must be bool")

            use_alternate_wave = (
                raw_use_alternate_wave
            )

            rule_product_code = str(
                data.get("product_code") or ""
            ).strip()

            if not rule_product_code:
                rule_product_code = (
                    product_code_from_name_and_spec(
                        product_code,
                        product_spec,
                    )
                )

            try:
                product_rule = resolve_product_rule(
                    product_code=rule_product_code,
                    layout=layout,
                    use_alternate_wave=use_alternate_wave,
                    product_count=product_count,
                )
            except ProductRuleError as exc:
                return False, str(exc)

            in_ids = (
                payload.get("InTareIDs")
                or payload.get("in_tare_ids")
                or data.get("in_tare_ids")
                or loc.get("in_tare_ids")
                or []
            )
            out_ids = (
                payload.get("OutTareIDs")
                or payload.get("out_tare_ids")
                or data.get("out_tare_ids")
                or loc.get("out_tare_ids")
                or []
            )

            # если в партии хранится только номер ячейки
            cell_no = payload.get("cell_no") or loc.get("cell_no") or data.get("cell_no")
            if cell_no is not None:
                if not in_ids:
                    in_ids = [cell_no]
                if not out_ids:
                    out_ids = [cell_no]

            in_ids = _as_int_list(in_ids)
            out_ids = _as_int_list(out_ids)

            # Семейство 312.229.001 во всех разрешённых исполнениях
            # всегда выгружается через ячейку 16.
            if uses_special_unloading_cell(product_rule.product_code):
                out_ids = [int(SPECIAL_UNLOADING_CELL)]

            if product_count <= 0 or not in_ids or not out_ids:
                return False, "missing fields for RTK_START"

            pn_base = product_rule.product_name
            product_spec = int(product_rule.product_spec)

            start_payload = {
              "ProductName": pn_base,
              "ProductSpec": int(product_spec), #pn_spec
              "ProductCount": int(product_count),
              "InTareIDs": [int(x) for x in in_ids],
              "OutTareIDs": [int(x) for x in out_ids],
              "Layout": int(layout),
              "GlobalMaxTareCount": int(
                  product_rule.global_max_tare_count
              ),
              "CurrentMaxTareCount": int(
                  product_rule.current_max_tare_count
              ),
              "UseAlternateWave": bool(use_alternate_wave),
            }

            # отправка start на РТК (уйдет через HttpRTK.poll_once)
            self.rtk.request("start", start_payload)
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={
                    "cmd": "start",
                    "payload": start_payload,
                    "batch_id": int(bid) if bid else None,
                    "operation_id": operation_id or getattr(st, "active_operation_id", None),                    
                },
            )

            # приводим систему/партию в "в работе"
            if st.mode != SystemMode.auto_running.value:
                _set_transient_message(
                    db,
                    "Автоматический режим запущен командой РТК",
                    Severity.info,
                    message_key="notice.system",
                    mode=SystemMode.auto_running.value,
                )
                self._resume_target_mode = SystemMode.auto_running.value
                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={"mode": SystemMode.auto_running.value},
                )

            if bid:
                repo.set_batch_status(db, int(bid), status="auto_processing", loaded_at=utcnow())
                _set_transient_message(
                    db,
                    f"Автоцикл начинается",
                    Severity.info,
                    message_key="notice.batch",
                    active_batch_id=int(bid),
                    active_batch_phase="starting",
                    active_batch_expected_count=int(product_count),
                    active_operation_id=operation_id or getattr(st, "active_operation_id", None),
                    active_operation_batch_id=int(bid),
                    active_operation_phase="rtk_start_sent",
                    active_operation_started_at=(getattr(st, "active_operation_started_at", None) or utcnow()),                    
                    last_meas_ok=None,
                    last_meas_not_ok=None,
                    last_meas_summary=None,
                    rtk_pickcount_seen=None,
                    rtk_defectcount_seen=None,
                    rtk_consecutive_defects=0,
                )
                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={"active_batch_id": int(bid), "active_batch_expected_count": int(product_count)},
                )

            # RTK_START только подготавливает tracking нового цикла.
            # Старый snapshot РТК в этот момент ещё может относиться
            # к предыдущей партии.
            if bid:
                if (
                    self._ppod_tripped_batch_id is not None
                    and int(self._ppod_tripped_batch_id) != int(bid)
                ):
                    self._clear_ppod_latch(
                        db,
                        reason="manual_rtk_start_other_batch",
                    )

                prestart_putcount = getattr(
                    snap,
                    "putcount",
                    None,
                )

                try:
                    prestart_putcount = (
                        None
                        if prestart_putcount is None
                        else int(prestart_putcount)
                    )
                except (TypeError, ValueError):
                    prestart_putcount = None

                old_context_batch = (
                    self._tare_track_batch_id
                    if self._tare_track_batch_id is not None
                    else self._tare_pending_batch_id
                )
                self._reset_tare_tracking(
                    reason=(
                        "rtk_start_other_batch"
                        if old_context_batch is not None
                        and int(old_context_batch) != int(bid)
                        else "rtk_start_prepare"
                    ),
                    batch_id=(
                        int(old_context_batch)
                        if old_context_batch is not None
                        else None
                    ),
                )

                self._tare_pending_batch_id = int(bid)
                self._tare_prestart_putcount = prestart_putcount
                self._tare_putcount_reset_seen = (
                    prestart_putcount == 0
                )

            return True, None


        if cmd_type == CommandType.RTK_SETSPEED.value:
            snap = self.rtk.snapshot()
            if not getattr(snap, "connected", False):
                return False, "rtk not connected"
        
            if "speed" not in payload and "value" in payload:
                payload = dict(payload)
                payload["speed"] = payload["value"]
        
            if "speed" not in payload:
                return False, "missing field speed"
        
            try:
                speed = int(payload["speed"])
            except Exception:
                return False, "speed must be int"
        
            if speed < 0:
                return False, "speed must be >= 0"
        
            p = {"speed": speed}
            self.rtk.request("setspeed", p)
        
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={"cmd": "setspeed", "payload": p},
            )
            return True, None
        
        if cmd_type == CommandType.RTK_SENDMEASUREMENTRESULT.value:
            self.rtk.request("send_measurement_result", payload)
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={"cmd": "send_measurement_result", "payload": payload},
            )
            return True, None
        
        if cmd_type == CommandType.RTK_SENDCALIBRATIONRESULT.value:
            self.rtk.request("send_calibration_result", payload)
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.RTK_COMMAND_SENT.value,
                payload={"cmd": "send_calibration_result", "payload": payload},
            )
            return True, None


        if cmd_type == CommandType.IM_MEASUREMENT_RESULT.value:
            payload = payload or {}
            st = repo.ensure_state_row(db)

            ok_ctx, bid, operation_id = self._resolve_operation_payload(
                db,
                payload,
                where="IM_MEASUREMENT_RESULT",
                allow_manual_without_operation=True,
            )
            if not ok_ctx:
                return True, None

            # 1) values/tols (готовые) или парсинг "сырого" payload
            values = payload.get("values")
            tols = payload.get("tols")
            
            if not (isinstance(values, dict) and isinstance(tols, dict)):
                try:
                    values, tols = parse_measurement_payload(payload)

                except Exception as e:
                    repo.add_event(
                        db,
                        severity=Severity.error.value,
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={
                            "where": "IM_MEASUREMENT_RESULT",
                            "err": str(e),
                            "payload_keys": list(payload.keys()),
                            "operation_id": operation_id,
                        },
                    )
                    return False, str(e)

            values = values or {}
            tols = tols or {}

            # 2) нормализуем tols к bool
            def _tol_ok(x) -> bool:
                if isinstance(x, bool):
                    return x
                if isinstance(x, (int, float)):
                    return bool(x)
                if isinstance(x, dict):
                    for k in ("ok", "pass", "in_tol", "inTol", "within_tol"):
                        if k in x:
                            return bool(x[k])
                if isinstance(x, str):
                    s = x.strip().lower()
                    if s in ("true", "ok", "pass", "1", "yes"):
                        return True
                    if s in ("false", "nok", "fail", "0", "no"):
                        return False
                return True

            values_n = {str(k): v for k, v in values.items()}
            tols_b = {str(k): _tol_ok(v) for k, v in tols.items()}

            # 3) если пришёл только ok/result - синтетическая "мера", чтобы UI не был пустым
            if not values_n and not tols_b:
                okv = payload.get("part_ok", payload.get("ok", payload.get("result")))
                if okv is not None:
                    values_n = {"mock": None}
                    tols_b = {"mock": bool(okv)}

            items = build_measurement_items(values_n, tols_b, decimals=4)

            not_ok = sum(1 for it in items if it["ok"] is False)
            part_ok = (not_ok == 0)

            summary = {"count": len(items), "shown": len(items), "items": items}

            # 4) обновляем state (для UI и для передачи в RTK)
            repo.set_state(
                db,
                last_meas_ok=1 if part_ok else 0,
                last_meas_not_ok=int(not_ok),
                last_meas_summary=json.dumps(summary, ensure_ascii=False),
                pending_mm_result=1 if part_ok else 0,
                mm_result_inflight=0,
            )

            measurement_message_key = (
                _batch_operator_message_key(
                    IM_WORKFLOW_MESSAGE_PREFIX,
                    bid,
                )
                or IM_WORKFLOW_MESSAGE_PREFIX
            )
            measurement_message = "Микрометр: измерение завершено"
            if bid is not None and operation_id:
                measurement_message += "; результат передаётся РТК"

            _set_message(
                db,
                measurement_message,
                Severity.info,
                message_key=measurement_message_key,
            )

            # consecutive_rejects + reject_item
            if part_ok:
                repo.set_state(db, consecutive_rejects=0)
            else:
                repo.set_state(db, consecutive_rejects=int(st.consecutive_rejects or 0) + 1)
                if bid is not None:
                    repo.add_reject_item(
                        db,
                        batch_id=int(bid),
                        measured_params={"items": items},
                        reason="measurement_nok",
                    )

            # 5) инкремент счётчиков партии (measured_good/measured_bad) и защита от накрутки сверх expected в авто
            expected = 0
            measured_after = None
            counted = None
            
            if bid is not None:
                b0 = repo.get_batch(db, int(bid))
                d0 = dict((b0.data or {}) if b0 else {})
                expected = int(st.active_batch_expected_count or d0.get("product_count") or 0)
                cur_measured = int(d0.get("measured_qty") or 0)
            
                in_auto_active = (st.mode == SystemMode.auto_running.value and int(st.active_batch_id or 0) == int(bid))
            
                if in_auto_active and expected > 0 and cur_measured >= expected:
                    counted = False
                    measured_after = cur_measured
                else:
                    counted = True
                    b1 = repo.inc_batch_measurement(db, int(bid), part_ok=part_ok)
                    d1 = dict((b1.data or {}) if b1 else {})
                    measured_after = int(d1.get("measured_qty") or cur_measured)

                    # PPOD: stop auto if defects reached limit
                    # current_batch_ppod = round_half_up(product_count * settings_ppod)
                    if in_auto_active and expected > 0 and (not part_ok) and b1:
                        mb1 = int(d1.get("measured_bad") or getattr(b1, "measured_bad", 0) or 0)

                        pp = float(self._settings_ppod or 0.0)
                        if pp < 0:
                            pp = 0.0
                        if pp > 1:
                            pp = 1.0

                        # half-up rounding
                        limit = int(math.floor((expected * pp) + 0.5))

                        if (
                            self._ppod_tripped_batch_id != int(bid)
                            and mb1 >= limit
                            and not self._deferred_stop_active(
                                db,
                                batch_id=bid,
                                operation_id=operation_id,
                            )
                        ):
                            self._ppod_tripped_batch_id = int(bid)

                            # Сохраняем причину и расчёт PPOD в партии.
                            # Эти данные понадобятся после безопасного завершения
                            # для сообщения оператору и событий BATCH_REJECTED/AUTO_STOPPED.
                            ppod_data = dict(b1.data or {})
                            ppod_data["ppod_exceeded"] = True
                            ppod_data["ppod_measured_bad"] = int(mb1)
                            ppod_data["ppod_limit"] = int(limit)
                            ppod_data["ppod_expected"] = int(expected)
                            ppod_data["ppod_settings"] = float(pp)
                            b1.data = ppod_data
                            flag_modified(b1, "data")
                            db.commit()

                            # В этот момент R2 обычно ещё ждёт текущий NOK-result.
                            # Сначала даём штатному tick отправить false в РТК,
                            # затем существующий deferred STOP переведёт партию
                            # в уже проверенный STOP_CLEANUP_PHASE.
                            r2_wait_state = (
                                self._rtk_r2_external_wait_state()
                                or "waitingmmresult"
                            )

                            ppod_message = (
                                "Превышен допустимый процент брака: "
                                f"брак {int(mb1)} из {int(expected)} шт., "
                                f"допустимый предел {int(limit)}. "
                                "Текущий результат будет передан РТК; "
                                "после этого роботы выполнят безопасное "
                                "завершение партии. Извлечение пока запрещено."
                            )

                            self._request_deferred_stop_after_external_result(
                                db,
                                batch_id=int(bid),
                                operation_id=operation_id,
                                reason="ppod_exceeded",
                                r2_state=r2_wait_state,
                                message=ppod_message,
                                severity=Severity.error,
                            )

                            repo.add_event(
                                db,
                                severity=Severity.error.value,
                                source="DAEMON",
                                type_=EventType.STATE_UPDATED.value,
                                payload={
                                    "message": "PPOD exceeded; safe stop requested",
                                    "ui_message": ppod_message,                                
                                    "batch_id": int(bid),
                                    "operation_id": operation_id,
                                    "measured_bad": int(mb1),
                                    "limit": int(limit),
                                    "expected": int(expected),
                                    "settings_ppod": float(pp),
                                    "r2_state": r2_wait_state,
                                },
                            )

      
            # 5.1) пишем запись измерения (всегда, даже если не засчитали в счётчики)
            repo.add_batch_measurement(
                db,
                batch_id=bid,
                part_ok=part_ok,
                not_ok=int(not_ok),
                values=values_n,
                tols=tols_b,
                items=items,
                counted=counted,
            )

            # 6) если добили count по ИМ - НЕ закрываем партию, а ждём, пока РТК вернётся в WaitingForStart у обоих роботов
            if bid is not None:
                st2 = repo.ensure_state_row(db)
                if st2.mode == SystemMode.auto_running.value and int(st2.active_batch_id or 0) == int(bid):
                    exp = int(st2.active_batch_expected_count or expected or 0)
                    if exp > 0 and int(measured_after or 0) >= exp:
                        if (st2.active_batch_phase or "") != "finishing":
                            _set_transient_message(
                                db,
                                f"Партия разобрана, ожидание РТК",
                                Severity.info,
                                message_key="notice.batch",
                                active_batch_phase="finishing",
                                active_operation_phase="finishing",
                            )
                            repo.add_event(
                                db,
                                severity=Severity.info.value,
                                source="DAEMON",
                                type_=EventType.STATE_UPDATED.value,
                                payload={
                                    "active_batch_id": int(bid),
                                    "operation_id": operation_id,
                                    "active_batch_phase": "finishing",
                                },
                            )                         

            return True, None

        
        if cmd_type == CommandType.IM_CALIBRATION_RESULT.value:  
            res = payload.get("result") # payload: {result: 0|-1|-2}
            if res is None and "ok" in payload:
                res = 0 if bool(payload["ok"]) else -1
            if res is None:
                return False, "result is required"
        
            res = int(res)
            st0 = repo.ensure_state_row(db)

            ok_ctx, batch_id, operation_id = self._resolve_operation_payload(
                db,
                payload,
                where="IM_CALIBRATION_RESULT",
                allow_manual_without_operation=True,
            )
            if not ok_ctx:
                return True, None

            reason = st0.calibration_reason
            calibration_message_key = (
                _batch_operator_message_key(
                    QUALITY_CALIBRATION_MESSAGE_PREFIX,
                    batch_id,
                )
                or QUALITY_CALIBRATION_MESSAGE_PREFIX
            )
            workflow_message_key = (
                _batch_operator_message_key(
                    IM_WORKFLOW_MESSAGE_PREFIX,
                    batch_id,
                )
                or IM_WORKFLOW_MESSAGE_PREFIX
            )

            need_switch = False
            target_full = None
            if res == 0 and batch_id:
                b = repo.get_batch(db, int(batch_id))
                if b:
                    d = b.data or {}
                    raw = d.get("product_code") or d.get("product_name") or ""
                    base, spec_from_code = parse_product_name_and_spec(str(raw))
                    spec = int(d.get("product_spec") or spec_from_code or 0)
                    full = (str(raw).strip() or base)
                    if spec and "-" not in str(raw):
                        full = f"{base}-{spec:02d}"
                    if spec != 0 and base and full and base != full:
                        need_switch = True
                        target_full = full
        
            # записали результат для гейта WaitingCalibrationResult
            repo.set_state(
                db,
                pending_calib_result=res,
                calib_result_inflight=0,
                calibration_inflight=0,
                calib_wait_im_program=(1 if need_switch else 0),
                calib_im_program_target=target_full
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.CALIBRATION_RESULT_RECORDED.value,
                payload={
                    "result": res,
                    "reason": reason,
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                },
            )

            reason_text = str(reason or "").lower()
            operation_name = (
                "проверка"
                if (
                    "retry_mode=2" in reason_text
                    or (
                        "three_consecutive" in reason_text
                        and "retry_mode=1" not in reason_text
                    )
                )
                else "калибровка"
            )

            if res == 0:
                self._clear_calibration_retry_safety_gate(db)

                # Успешный результат является текущим workflow ИМ, а не
                # итоговым quality-сообщением партии. Предыдущее сообщение
                # о запросе проверки заменяем одним актуальным статусом.
                _clear_batch_operator_messages(
                    db,
                    batch_id,
                    QUALITY_CALIBRATION_MESSAGE_PREFIX,
                )
                _set_message(
                    db,
                    f"Микрометр: {operation_name} завершена успешно; результат передаётся РТК",
                    Severity.info,
                    message_key=workflow_message_key,
                )
            else:
                # Для -1/-3/-2/-4 workflow завершён; дальше оператору
                # показывается отдельное quality-сообщение результата.
                _clear_batch_operator_messages(
                    db,
                    batch_id,
                    IM_WORKFLOW_MESSAGE_PREFIX,
                )

            if need_switch and target_full:
                db.add(CommandRow(
                    type=CommandType.IM_LOAD_PROGRAM.value,
                    status=CommandStatus.pending.value,
                    payload={
                        "product_code": str(target_full),
                        "batch_id": int(batch_id),
                        "operation_id": operation_id,                        
                        "after_calibration": 1,
                        "phase": "after_calibration",
                        "delay_before_load_sec": 3.0,
                    },
                    created_by="DAEMON",
                ))
                db.commit()

            # Если оператор нажал СТОП во время waitingcalibrationresult,
            # не выполняем штатную аварийную обработку калибровки здесь.
            # Сначала tick() должен отправить pending_calib_result в РТК,
            # затем R2 выйдет из waitingcalibrationresult,
            # и только после этого сработает отложенный STOP.
            if res != 0 and self._deferred_stop_active(
                db,
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "calibration failure handling deferred because STOP_AUTO is waiting R2 gate close",
                        "result": int(res),
                        "batch_id": batch_id,
                        "operation_id": operation_id,
                    },
                )
                return True, None
        
            # -1: неудачная калибровка ИЛИ проверка. Результат сначала
            # уходит в РТК через штатный gate waitingcalibrationresult,
            # после чего РТК перейдёт в paused. После осмотра, восстановления
            # аварийного контура и ручного ПРОДОЛЖИТЬ повторяем именно ту
            # операцию ИМ, которая завершилась результатом -1:
            # mode=1 для калибровки, mode=2 для проверки.
            if res == -1:
                st = repo.ensure_state_row(db)
                active_batch_id = (
                    int(batch_id)
                    if batch_id is not None
                    else (
                        int(st.active_batch_id)
                        if st.active_batch_id is not None
                        else None
                    )
                )

                retry_mode = (
                    2
                    if operation_name == "проверка"
                    else 1
                )

                base_reason = re.sub(
                    r"\s*;?\s*retry_mode=[12]\s*",
                    "",
                    str(reason or ""),
                )
                base_reason = re.sub(
                    rf"\s*;?\s*{re.escape(CHECK_TO_CALIBRATION_MARKER)}\s*",
                    "",
                    base_reason,
                ).strip(" ;")

                retry_reason = (
                    f"{base_reason}; retry_mode={retry_mode}"
                    if base_reason
                    else f"retry_mode={retry_mode}"
                )

                repo.set_state(
                    db,
                    calibration_reason=retry_reason,
                    consecutive_rejects=0,
                    rtk_consecutive_defects=0,
                )
                self._set_calibration_retry_safety_gate(
                    db,
                    batch_id=active_batch_id,
                    operation_id=operation_id,
                )

                message = (
                    f"Неудачная {operation_name}: результат -1. "
                    "Проверьте оборудование: нажмите аварийную кнопку, "
                    "проведите осмотр, восстановите аварийный контур "
                    f"и нажмите ПРОДОЛЖИТЬ. После этого {operation_name} "
                    "будет повторена."
                )
                _set_message(
                    db,
                    message,
                    Severity.warn,
                    message_key=calibration_message_key,
                )

                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": message,
                        "where": "CALIBRATION_RETRY_REQUIRED",
                        "result": -1,
                        "batch_id": active_batch_id,
                        "operation_id": operation_id,
                        "rtk_pause_expected": True,
                        "next_calib_mode": int(retry_mode),
                    },
                )
                return True, None

            # -3: проверка не пройдена. Оператор в ячейку не входит.
            # После отправки -3 в РТК автоматически выполняем _rtk_resume(),
            # а следующую операцию ИМ запускаем уже как калибровку mode=1.
            if res == -3:
                st = repo.ensure_state_row(db)
                active_batch_id = (
                    int(batch_id)
                    if batch_id is not None
                    else (
                        int(st.active_batch_id)
                        if st.active_batch_id is not None
                        else None
                    )
                )

                # Удаляем возможный старый retry_mode=2 от проверки
                # и принудительно назначаем следующую операцию mode=1.
                base_reason = re.sub(
                    r"\s*;?\s*retry_mode=[12]\s*",
                    "",
                    str(reason or ""),
                )
                base_reason = re.sub(
                    rf"\s*;?\s*{re.escape(CHECK_TO_CALIBRATION_MARKER)}\s*",
                    "",
                    base_reason,
                ).strip(" ;")

                retry_reason = (
                    f"{base_reason}; retry_mode=1; "
                    f"{CHECK_TO_CALIBRATION_MARKER}"
                    if base_reason
                    else (
                        f"retry_mode=1; "
                        f"{CHECK_TO_CALIBRATION_MARKER}"
                    )
                )

                self._clear_calibration_retry_safety_gate(db)

                repo.set_state(
                    db,
                    calibration_reason=retry_reason,
                    consecutive_rejects=0,
                    rtk_consecutive_defects=0,
                )

                message = (
                    "Проверка не пройдена: результат -3. "
                    "Автоматически начинаем калибровку; "
                    "вход оператора в ячейку не требуется."
                )
                _set_message(
                    db,
                    message,
                    Severity.warn,
                    message_key=calibration_message_key,
                )

                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": message,
                        "where": "CHECK_FAILED_START_CALIBRATION",
                        "result": -3,
                        "batch_id": active_batch_id,
                        "operation_id": operation_id,
                        "rtk_resume_after_result": True,
                        "next_calib_mode": 1,
                    },
                )
                return True, None

            # -2/-4: фатальная ошибка. Завершаем текущую операцию через
            # штатную аварийную процедуру, внутри которой вызывается _rtk_stop(),
            # выключается вакуум и партия переводится в rejected.
            if res in (-2, -4):
                self._clear_calibration_retry_safety_gate(db)
                st = repo.ensure_state_row(db)
                active_batch_id = (
                    int(batch_id)
                    if batch_id is not None
                    else (
                        int(st.active_batch_id)
                        if st.active_batch_id is not None
                        else None
                    )
                )
                fatal_reason = f"calibration_fatal; result={int(res)}"


                # Даже фатальный результат -2/-4 сначала должен уйти
                # на РТК через etalon_result. Только после него ставим STOP.
                # HttpRTK использует FIFO-очередь, поэтому вызываемый ниже
                # _rtk_stop() добавит STOP строго после результата калибровки.
                self.rtk.request(
                    "send_calibration_result",
                    {"result": int(res)},
                )

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.RTK_COMMAND_SENT.value,
                    payload={
                        "cmd": "send_calibration_result",
                        "result": int(res),
                        "batch_id": active_batch_id,
                        "operation_id": operation_id,
                        "reason": "fatal_calibration_result_before_stop",
                    },
                )

                # Результат уже напрямую поставлен в очередь RTK.
                # pending_calib_result очищаем, чтобы tick() не поставил его
                # повторно, но сохраняем calib_result_inflight=1 до выхода R2
                # из waitingcalibrationresult. Иначе STOP-cleanup на том же
                # stale snapshot создаст лишний synthetic result=0, который
                # после STOP станет недопустимой головой FIFO и заблокирует
                # последующие start/cycle_on/pause/resume/reset_* команды.
                repo.set_state(
                    db,
                    pending_calib_result=None,
                    calib_result_inflight=1,
                    calibration_inflight=0,
                )

                if active_batch_id is None:
                    self._rtk_stop(db, "calibration_fatal")
                    _set_message(
                        db,
                        f"Фатальная ошибка калибровки/проверки: результат {res}",
                        Severity.error,
                        message_key=calibration_message_key,
                    )

                    repo.add_event(
                        db,
                        severity=Severity.error.value,
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={
                            "where": "CALIBRATION_FATAL",
                            "result": int(res),
                            "reason": reason,
                            "batch_id": None,
                            "operation_id": operation_id,
                        },
                    )
                    return True, None

                fatal_message = (
                    "Фатальная ошибка калибровки/проверки "
                    f"(результат {res}). Партия отклонена; "
                    "дождитесь остановки РТК и извлеките партию."
                )
                _set_message(
                    db,
                    fatal_message,
                    Severity.error,
                    message_key=calibration_message_key,
                )

                ok_abort, err_abort = self.handle_command(
                    db,
                    "ABORT_ACTIVE_BATCH_FOR_EXTRACTION",
                    {
                        "batch_id": int(active_batch_id),
                        "reason": fatal_reason,
                        "reject_reason": fatal_reason,
                        "rtk_stop_reason": "calibration_fatal",
                        "source": "IM_CALIBRATION_RESULT",
                        "defer_if_r2_waiting": False,
                        "message": fatal_message,
                    },
                )


                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "CALIBRATION_FATAL",
                        "result": int(res),
                        "reason": reason,
                        "batch_id": int(active_batch_id),
                        "operation_id": operation_id,
                        "abort_ok": bool(ok_abort),
                        "abort_error": err_abort,
                    },
                )

                return ok_abort, err_abort

            if res != 0:
                return False, f"unsupported calibration result: {res}"
        
            return True, None

        # -------- operator/manual batch controls --------

        if cmd_type == CommandType.SET_ACTIVE_BATCH.value:
            ok, err = self._can_resume(db)
            if not ok:
                return False, err

            bid = int((payload or {}).get("batch_id") or 0)
            if not bid:
                return False, "payload.batch_id is required"

            if not repo.get_batch(db, bid):
                return False, f"batch not found: {bid}"

            st = repo.ensure_state_row(db)
            if (
                st.mode == SystemMode.auto_running.value
                or bool(str(getattr(st, "active_operation_id", "") or "").strip())
            ):
                active_bid = getattr(st, "active_operation_batch_id", None)
                if active_bid is None:
                    active_bid = st.active_batch_id

                if active_bid is not None and int(active_bid) == int(bid):
                    return True, None

                return (
                    False,
                    "active_batch_id нельзя менять во время автоцикла; "
                    "для извлечения используйте MARK_ACTIVE_BATCH_EXTRACTED с payload.batch_id",
                )

            if not bool(st.safety_ok):
                return False, "safety tripped"
            if not bool(st.trash_present):
                return False, "trash missing"

            _set_transient_message(
                db,
                f"Партия выбрана как активная",
                Severity.info,
                message_key="notice.batch",
                active_batch_id=bid,
                active_batch_phase=None,
                last_meas_ok=None,
                last_meas_not_ok=None,
                last_meas_summary=None,
            )
            repo.add_event(db, severity=Severity.info.value, source="DAEMON",
                           type_=EventType.STATE_UPDATED.value, payload={"active_batch_id": bid})
            return True, None

        if cmd_type == CommandType.POSTAMAT_OPEN_LOADING_CELL.value:
            extraction_ok, extraction_err = (
                self._validate_extraction_open_payload(
                    db,
                    payload,
                )
            )
            if not extraction_ok:
                return False, extraction_err

            ok, err = self._can_open_postamat_cell(
                db,
                payload,
            )
            if not ok:
                return False, err

            idx = int((payload or {}).get("idx") or 0)
            if not idx:
                return False, "payload.idx is required"

            blocked, block_msg = self._cell_open_blocked_by_rtk_waiting_stop(
                db,
                idx=int(idx),
                side="loading",
            )
            if blocked:
                return False, block_msg

            self.io.request_open_loading_cell(idx)
            repo.add_event(db, severity=Severity.info.value, source="DAEMON",
                           type_=EventType.POSTAMAT_CELL_OPEN_REQUESTED.value,
                           payload={"side": "loading", "idx": idx})
            return True, None

        if cmd_type == CommandType.POSTAMAT_OPEN_UNLOADING_CELL.value:
            extraction_ok, extraction_err = (
                self._validate_extraction_open_payload(
                    db,
                    payload,
                )
            )
            if not extraction_ok:
                return False, extraction_err

            ok, err = self._can_open_postamat_cell(
                db,
                payload,
            )
            if not ok:
                return False, err

            idx = int((payload or {}).get("idx") or 0)
            if not idx:
                return False, "payload.idx is required"

            blocked, block_msg = self._cell_open_blocked_by_rtk_waiting_stop(
                db,
                idx=int(idx),
                side="unloading",
            )
            if blocked:
                return False, block_msg

            self.io.request_open_unloading_cell(idx)
            repo.add_event(db, severity=Severity.info.value, source="DAEMON",
                           type_=EventType.POSTAMAT_CELL_OPEN_REQUESTED.value,
                           payload={"side": "unloading", "idx": idx})
            return True, None

        if cmd_type == CommandType.CHECK_BATCH_EXTRACTION_DOORS.value:
            try:
                batch_id = int(
                    (payload or {}).get("batch_id") or 0
                )
            except (TypeError, ValueError):
                batch_id = 0

            ok, err = self._check_batch_extraction_doors(
                db,
                batch_id=batch_id,
            )

            if not ok:
                return False, err

            return True, None

        if cmd_type == CommandType.PRINT_DOCS.value:
            payload = payload or {}

            batch_id = int(payload.get("batch_id") or 0)
            docs = payload.get("docs") or []

            if not batch_id:
                return False, "payload.batch_id is required"
            if not isinstance(docs, list) or not docs:
                return False, "payload.docs must be non-empty list"

            doc_names, unknown = _normalize_print_docs(docs)
            if unknown:
                return False, f"unknown docs: {unknown}"

            b = repo.get_batch(db, batch_id)
            if not b:
                return False, f"batch {batch_id} not found"

            if b.status not in {"done", "rejected", "extracted"}:
                return False, (
                    f"batch {batch_id} docs cannot be printed from status {b.status}; "
                    "allowed statuses: done, rejected, extracted"
                )

            try:
                db.refresh(b)
            except Exception:
                pass

            # Во время извлечения двери уже могут быть открыты.
            # Поэтому печать документов НЕ должна зависеть от _can_resume(),
            # safety_ok/trash_present/температуры/воздуха.
            # Документы строятся только по данным партии в БД.

            self._maybe_finalize_open_tare(db, b, reason="print")
            try:
                db.refresh(b)
            except Exception:
                pass

            printed: dict[str, str | list[str]] = {}
            skipped: list[dict] = []
            protocol_text: str | None = None
            tares = _norm_out_tares(b)
            document_prefix = _batch_document_prefix(b)

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": "print docs started",
                    "batch_id": int(batch_id),
                    "requested_docs": docs,
                    "docs": doc_names,
                    "printer": self.printer.__class__.__name__,
                    "spool_dir": str(getattr(self.printer, "spool_dir", "")),
                },
            )

            try:
                for doc_name in doc_names:
                    if doc_name == "protocol":
                        if protocol_text is None:
                            protocol_text = _build_protocol_text(
                                b,
                                operator_name=str(created_by or "-"),
                            )

                        printed[doc_name] = self.printer.print_text(
                            protocol_text,
                            job_name=f"{document_prefix}_protocol",
                        )

                    elif doc_name == "defect_protocol":
                        txt_def = self._defect_protocol_text_for_batch(
                            db,
                            b,
                            operator_name=str(created_by or "-"),
                        )

                        # После правки _build_defect_protocol_text обычно
                        # возвращает текст всегда. Но fallback оставляем,
                        # чтобы команда печати не молчала.
                        if not txt_def:
                            txt_def = "\n".join([
                                "Протокол брака партии",
                                "",
                                f"Партия:                       {document_prefix}",
                                f"Оператор:                     {str(created_by or '-')}",
                                "",
                                "Дефекты по партии не обнаружены.",
                                "",
                                f"Дата и время печати протокола:{_dt_now_local_str()}",
                            ])

                        printed[doc_name] = self.printer.print_text(
                            txt_def,
                            job_name=f"{document_prefix}_defect_protocol",
                        )

                    elif doc_name == "labels":
                        paths: list[str] = []
    
                        d = b.data or {}
                        mg, mb = _get_counts(b)
    
                        # Fallback нужен только если по каким-то причинам нет out_tares.
                        # Для ярлыков берём именно годные детали, а не все измеренные.
                        fallback_total = (
                            int(mg or 0)
                            or int(d.get("measured_good") or 0)
                            or int(d.get("ok_qty") or 0)
                            or int(d.get("product_count") or 0)
                        )
    
                        # Группируем тары по ячейке.
                        # Для каждой ячейки печатаем один файл:
                        #   6_label_05_...
                        by_cell: dict[int, list[dict]] = {}
    
                        for t in tares:
                            try:
                                cell_no = int(t["cell_no"])
                                tare_no = int(t["tare_no"])
                                qty = int(t["qty"])
                            except Exception:
                                continue
    
                            if cell_no <= 0 or tare_no <= 0 or qty <= 0:
                                continue
    
                            by_cell.setdefault(cell_no, []).append({
                                "cell_no": cell_no,
                                "tare_no": tare_no,
                                "qty": qty,
                            })
    
                        if by_cell:
                            for cell_no in sorted(by_cell.keys()):
                                cell_tares = sorted(
                                    by_cell[cell_no],
                                    key=lambda x: int(x["tare_no"]),
                                )
    
                                txt = _build_labels_sheet_text(
                                    b,
                                    cell_tares,
                                )
    
                                if not txt:
                                    continue
    
                                paths.append(
                                    self.printer.print_text(
                                        txt,
                                        job_name=f"{document_prefix}_label_{cell_no:02d}",
                                    )
                                )
                        else:
                            # Старый/fallback-сценарий, если нет информации по тарам.
                            # Печатаем один ярлык на первую ячейку выгрузки.
                            cell_no = _first_out_cell(b)
    
                            fallback_tare = {
                                "cell_no": int(cell_no or 0),
                                "tare_no": 1,
                                "qty": int(fallback_total),
                            }
    
                            txt = _build_labels_sheet_text(
                                b,
                                [fallback_tare],
                            )
    
                            if txt:
                                job_name = (
                                    f"{document_prefix}_label_{int(cell_no):02d}"
                                    if int(cell_no or 0) > 0
                                    else f"{document_prefix}_label"
                                )
    
                                paths.append(
                                    self.printer.print_text(
                                        txt,
                                        job_name=job_name,
                                    )
                                )
    
                        printed[doc_name] = paths

            except Exception as e:
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "PRINT_DOCS",
                        "err": str(e),
                        "batch_id": int(batch_id),
                        "requested_docs": docs,
                        "docs": doc_names,
                        "printer": self.printer.__class__.__name__,
                        "spool_dir": str(getattr(self.printer, "spool_dir", "")),
                        "printed_before_error": printed,
                    },
                )
                return False, f"print docs failed: {e}"

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.DOCS_PRINTED.value,
                payload={
                    "batch_id": int(batch_id),
                    "requested_docs": docs,
                    "docs": doc_names,
                    "paths": printed,
                    "skipped": skipped,
                    "printer": self.printer.__class__.__name__,
                    "spool_dir": str(getattr(self.printer, "spool_dir", "")),
                },
            )

            if str(payload.get("purpose") or "") != "extraction":
                _set_transient_message(
                    db,
                    f"Документы партии {document_prefix} отправлены на печать",
                    Severity.info,
                    message_key="notice.print",
                )

            return True, None


        if cmd_type == "ABORT_ACTIVE_BATCH_FOR_EXTRACTION":
            st = repo.ensure_state_row(db)

            bid = int((payload or {}).get("batch_id") or 0)
            if not bid:
                return False, "payload.batch_id is required"

            b = repo.get_batch(db, bid)
            if not b:
                return False, f"batch {bid} not found"

            if b.status != "auto_processing":
                return False, (
                    f"batch {bid} emergency abort is allowed only from "
                    f"auto_processing, current status: {b.status}"
                )

            active_bid = getattr(
                st,
                "active_operation_batch_id",
                None,
            )
            if active_bid is None:
                active_bid = st.active_batch_id

            if active_bid is None or int(active_bid) != int(bid):
                return False, (
                    f"batch {bid} is not current active batch; "
                    f"active_batch_id={st.active_batch_id}, "
                    f"active_operation_batch_id="
                    f"{getattr(st, 'active_operation_batch_id', None)}"
                )

            current_data = dict(b.data or {})
            if (
                str(st.active_batch_phase or "")
                == STOP_CLEANUP_PHASE
                or bool(current_data.get("stop_cleanup_active"))
            ):
                _set_message(
                    db,
                    (
                        f"СТОП партии уже выполняется; "
                        "ожидаем безопасное завершение роботов"
                    ),
                    Severity.warn,
                    message_key=(
                        _batch_operator_message_key(
                            STOP_MESSAGE_PREFIX,
                            bid,
                        )
                        or STOP_MESSAGE_PREFIX
                    ),
                )
                return True, None               

            abort_reason = str(
                (payload or {}).get("reason")
                or "operator_abort_batch_for_extraction"
            )
            rtk_stop_reason = str(
                (payload or {}).get("rtk_stop_reason")
                or abort_reason
            )
            reject_reason = str(
                (payload or {}).get("reject_reason")
                or abort_reason
            )
            source_cmd = str(
                (payload or {}).get("source")
                or "ABORT_ACTIVE_BATCH_FOR_EXTRACTION"
            )


            operation_id = str(
                getattr(st, "active_operation_id", "")
                or ""
            ).strip()
            operation_phase = getattr(
                st,
                "active_operation_phase",
                None,
            )

            interrupted: list[str] = []
            if bool(getattr(st, "mm_result_inflight", 0)):
                interrupted.append("измерение")
            if (
                bool(getattr(st, "calibration_inflight", 0))
                or bool(getattr(st, "calib_result_inflight", 0))
            ):
                interrupted.append("калибровка/проверка")

            # Если R2 ждёт результат ИМ/калибровки, сохраняем
            # старую безопасную схему: сначала закрываем gate результата.
            r2_wait_state = self._rtk_r2_external_wait_state()
            if (
                bool(
                    (payload or {}).get(
                        "defer_if_r2_waiting",
                        False,
                    )
                )
                and r2_wait_state
            ):
                self._request_deferred_stop_after_external_result(
                    db,
                    batch_id=int(bid),
                    operation_id=operation_id,
                    reason=abort_reason,
                    r2_state=r2_wait_state,
                )
                return True, None

            try:
                snap_stop = self.rtk.snapshot()
                rtk_connected_at_stop = bool(
                    getattr(snap_stop, "connected", False)
                )
                r2_action_at_stop = str(
                    getattr(snap_stop, "action_r2", "")
                    or ""
                ).strip().lower()
                defectcount_at_stop = getattr(
                    snap_stop,
                    "defectcount",
                    None,
                )
            except Exception:
                rtk_connected_at_stop = False
                r2_action_at_stop = ""
                defectcount_at_stop = None

            # Отправляем STOP, но пока НЕ переводим партию
            # в rejected и НЕ разрешаем открытие ячеек.
            self._rtk_stop(db, rtk_stop_reason)

            try:
                defectcount_start = (
                    None
                    if defectcount_at_stop is None
                    else int(defectcount_at_stop)
                )
            except Exception:
                defectcount_start = None

            stop_data = dict(b.data or {})
            stop_data["abort_reason"] = reject_reason
            stop_data["abort_source"] = source_cmd
            stop_data["abort_operation_id"] = operation_id
            stop_data["abort_operation_phase"] = operation_phase
            stop_data["abort_interrupted"] = list(interrupted)

            stop_data["stop_cleanup_active"] = True
            stop_data["stop_cleanup_started_at"] = (
                utcnow().isoformat()
            )
            stop_data["stop_cleanup_defectcount_at_start"] = (
                defectcount_start
            )
            stop_data["stop_cleanup_last_defectcount"] = (
                defectcount_start
            )
            stop_data["unmeasured_after_stop_qty"] = int(
                stop_data.get(
                    "unmeasured_after_stop_qty",
                    0,
                )
                or 0
            )

            stop_data["extraction_ready"] = False
            stop_data["extraction_wait_rtk_safe"] = True
            stop_data["extraction_blocked_rtk_state"] = (
                r2_action_at_stop or None
            )

            b.data = stop_data
            flag_modified(b, "data")
            db.commit()

            # Старый operation_id очищаем: поздние результаты ИМ
            # должны быть отброшены guard-проверками как stale.
            self._mm_req_sent_batch = None
            self._calib_req_sent_batch = None
            self._calib_program_load_req_key = None

            self._calib_due_at = None
            self._calib_due_batch = None
            self._calib_due_operation_id = None
            self._calib_due_reject_threshold = None

            # Для PPOD сохраняем latch на всём этапе безопасного
            # завершения: color=4/sound=3 должны действовать до момента,
            # когда оба робота закончат операции. Обычный операторский STOP
            # остаётся без изменений.
            if (
                abort_reason != "ppod_exceeded"
                and self._ppod_tripped_batch_id is not None
                and int(self._ppod_tripped_batch_id) == int(bid)
            ):
                self._clear_ppod_latch(
                    db,
                    reason=abort_reason,
                )

            self._clear_deferred_stop(db)            

            _clear_batch_operator_messages(
                db,
                bid,
                IM_WORKFLOW_MESSAGE_PREFIX,
            )

            if not abort_reason.startswith("calibration_fatal"):
                _clear_batch_operator_messages(
                    db,
                    bid,
                    QUALITY_CALIBRATION_MESSAGE_PREFIX,
                )

            is_ppod_stop = (
                abort_reason == "ppod_exceeded"
                or bool(stop_data.get("ppod_exceeded"))
            )

            if is_ppod_stop:
                ppod_bad = int(
                    stop_data.get("ppod_measured_bad") or 0
                )
                ppod_limit = int(
                    stop_data.get("ppod_limit") or 0
                )
                ppod_expected = int(
                    stop_data.get("ppod_expected") or 0
                )

                quality_msg = (
                    "Превышен допустимый процент брака"
                )
                if ppod_expected > 0:
                    quality_msg += (
                        f": брак {ppod_bad} из {ppod_expected} шт., "
                        f"допустимый предел {ppod_limit}"
                    )
                quality_msg += (
                    ". STOP отправлен РТК. Роботы завершают "
                    "опасные операции и перемещают оставшиеся "
                    "детали в тару брака. Извлечение будет "
                    "доступно только после безопасного останова "
                    "обоих роботов."
                )
                _set_message(
                    db,
                    quality_msg,
                    Severity.error,
                    message_key=(
                        _batch_operator_message_key(
                            QUALITY_PPOD_MESSAGE_PREFIX,
                            bid,
                        )
                        or QUALITY_PPOD_MESSAGE_PREFIX
                    ),
                )

                msg = (
                    f"СТОП принят. "
                    "Роботы завершают опасные операции и перемещают "
                    "оставшиеся детали в тару брака. "
                    "Извлечение будет доступно после безопасного "
                    "останова обоих роботов."
                )
                msg_severity = Severity.warn
            else:
                msg = (
                    f"СТОП принят. "
                    "Роботы завершают опасные операции и перемещают "
                    "оставшиеся детали в тару брака. "
                    "Извлечение будет доступно после безопасного "
                    "останова обоих роботов."
                )
                msg_severity = Severity.warn

            if interrupted:
                msg += (
                    "; Прервана операция: "
                    + ", ".join(interrupted)
                    + "."
                )

            if not rtk_connected_at_stop:
                msg += (
                    " ВНИМАНИЕ: РТК не на связи; извлечение "
                    "останется запрещено до подтверждённого "
                    "безопасного состояния роботов."
                )
                msg_severity = Severity.error            

            _set_message(
                db,
                msg,
                msg_severity,
                message_key=(
                    _batch_operator_message_key(
                        STOP_MESSAGE_PREFIX,
                        bid,
                    )
                    or STOP_MESSAGE_PREFIX
                ),
                active_batch_phase=STOP_CLEANUP_PHASE,
                active_operation_id=None,
                active_operation_batch_id=None,
                active_operation_phase=None,
                active_operation_started_at=None,
                pending_mm_result=None,
                mm_result_inflight=0,
                pending_calib_result=None,
                calib_result_inflight=(
                    1
                    if (
                        abort_reason.startswith("calibration_fatal")
                        and bool(getattr(st, "calib_result_inflight", 0))
                    )
                    else 0
                ),
                calibration_inflight=0,
                calibration_reason=None,
                calib_wait_im_program=0,
                calib_im_program_target=None,
                consecutive_rejects=0,
            )


            repo.add_event(
                db,
                severity=msg_severity.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": msg,
                    "message_key": _batch_operator_message_key(
                        STOP_MESSAGE_PREFIX,
                        bid,
                    ),
                    "quality_message": (
                        quality_msg
                        if is_ppod_stop
                        else None
                    ),
                    "batch_id": int(bid),                    
                    "source_cmd": source_cmd,
                    "active_batch_phase": STOP_CLEANUP_PHASE,
                    "extraction_ready": False,
                    "rtk_connected_at_stop": bool(
                        rtk_connected_at_stop
                    ),
                    "r2_action_at_stop": r2_action_at_stop,
                    "defectcount_at_stop": defectcount_at_stop,                    
                },
            )

            return True, None


        if cmd_type == CommandType.MARK_ACTIVE_BATCH_EXTRACTED.value:
            st = repo.ensure_state_row(db)

            bid = int((payload or {}).get("batch_id") or 0)
            if not bid:
                return False, "payload.batch_id is required"

            b = repo.get_batch(db, bid)

            if not b:
                return False, f"batch {bid} not found"        

            allowed_extract_statuses = {
                "done",
                "rejected",
            }

            if b.status not in allowed_extract_statuses:
                return False, (
                    f"batch {bid} cannot be extracted from status {b.status}; "
                    f"allowed statuses: {sorted(allowed_extract_statuses)}. "
                    "For active auto_processing batch press STOP_AUTO first."
                )

            extraction_data = dict(b.data or {})
            if extraction_data.get("extraction_ready") is False:
                return False, (
                    f"Извлечение партии временно запрещено: "
                    "роботы ещё завершают безопасную остановку"
                )

            doors_ok, doors_err = (
                self._check_batch_extraction_doors(
                    db,
                    batch_id=bid,
                )
            )
            if not doors_ok:
                return False, doors_err

            # чтобы extracted_out_tares забрал полный актуальный out_tares
            try:
                db.refresh(b)
            except Exception:
                pass

            self._maybe_finalize_open_tare(db, b, reason="extract")
            try:
                db.refresh(b)
            except Exception:
                pass

            def _as_int_list(v) -> list[int]:
                if v is None:
                    return []
                if isinstance(v, list):
                    items = v
                else:
                    items = [v]
                out: list[int] = []
                for x in items:
                    try:
                        out.append(int(x))
                    except Exception:
                        continue
                return out

            # определить ячейки партии (loading/unloading)
            data = dict(b.data or {})
            loc = dict(b.location or {})
            in_ids = _as_int_list(loc.get("in_tare_ids") or data.get("in_tare_ids"))
            out_ids = _as_int_list(loc.get("out_tare_ids") or data.get("out_tare_ids"))

            # Если извлекается партия, на которой висит PPOD-индикация,
            # снимаем latch. Важно: _clear_ppod_latch НЕ должен сбрасывать
            # _last_sound_cmd/_last_color_cmd.
            if (
                self._ppod_tripped_batch_id is not None
                and int(self._ppod_tripped_batch_id) == int(bid)
            ):
                self._clear_ppod_latch(db, reason="rejected_batch_extracted")

            # помечаем партию извлеченной и освобождаем ячейку, чтобы можно было загрузить новую партию
            b.status = "extracted"
            b.extracted_at = utcnow()
            b.updated_at = utcnow()

            # сохраним откуда извлекали (для истории), но уберём occupancy-поля
            if in_ids and "extracted_from_in_tare_ids" not in data:
                data["extracted_from_in_tare_ids"] = in_ids
            if out_ids and "extracted_from_out_tare_ids" not in data:
                data["extracted_from_out_tare_ids"] = out_ids
           
            # сохранить раскладку по таре (для истории/печати после извлечения)
            if (data.get("out_tares") or data.get("extracted_out_tares")) and "extracted_out_tares" not in data:
                src = data.get("out_tares") or data.get("extracted_out_tares")
                data["extracted_out_tares"] = json.loads(json.dumps(src))
            
            if "in_tare_ids" in data:
                data["in_tare_ids"] = []
            if "out_tare_ids" in data:
                data["out_tare_ids"] = []

            b.data = data
            b.location = {}
            db.commit()

            clear_active_refs = (
                st.active_batch_id is not None
                and int(st.active_batch_id) == int(bid)
            )

            _clear_batch_operator_messages(
                db,
                bid,
                QUALITY_PPOD_MESSAGE_PREFIX,
                QUALITY_CALIBRATION_MESSAGE_PREFIX,
                STOP_MESSAGE_PREFIX,
                IM_WORKFLOW_MESSAGE_PREFIX,
                BATCH_RESULT_MESSAGE_PREFIX,
                BATCH_START_MESSAGE_PREFIX,
            )

            if clear_active_refs:
                repo.set_state(
                    db,
                    mode=SystemMode.idle.value,
                    active_batch_id=None,
                    active_batch_phase=None,
                    active_batch_expected_count=None,
                    active_operation_id=None,
                    active_operation_batch_id=None,
                    active_operation_phase=None,
                    active_operation_started_at=None,
                    pending_mm_result=None,
                    mm_result_inflight=0,
                    pending_calib_result=None,
                    calib_result_inflight=0,
                    calibration_inflight=0,
                    calibration_reason=None,
                    calib_wait_im_program=0,
                    calib_im_program_target=None,
                    rtk_pickcount_seen=None,
                    rtk_defectcount_seen=None,
                    rtk_consecutive_defects=0,
                )

                self._resume_target_mode = None
                self._mm_req_sent_batch = None
                self._calib_req_sent_batch = None
                self._calib_program_load_req_key = None
                self._calib_due_at = None
                self._calib_due_batch = None
                self._calib_due_operation_id = None
                self._calib_due_reject_threshold = None
                self._reset_tare_tracking(
                    reason="active_batch_extracted",
                    batch_id=bid,
                )
            repo.clear_operator_message(db, "notice.extraction")
            repo.clear_operator_message(db, "notice.print")

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.BATCH_EXTRACTED.value,
                payload={"batch_id": bid, "freed_loading": in_ids, "freed_unloading": out_ids},
            )
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "active_batch_id": (None if clear_active_refs else st.active_batch_id),
                    "extracted_batch_id": bid,
                    "active_refs_cleared": bool(clear_active_refs),
                },
            )

            # В штатной процедуре MARK_ACTIVE_BATCH_EXTRACTED вызывается
            # только после успешной PRINT_DOCS. Ошибка UDP не отменяет
            # уже подтверждённое извлечение и освобождение ячеек.
            self._send_extracted_batch_udp(db, b)

            return True, None
            
        return False, f"unknown command type: {cmd_type}"


    def _send_extracted_batch_udp(
        self,
        db: Session,
        batch: BatchRow,
    ) -> None:
        """
        Передаёт данные извлечённой партии во внешнюю систему.

        UDP не должен влиять на освобождение ячеек и статус партии:
        отсутствие настроек или ошибка сети только фиксируются в events.
        """
        server_ip = str(
            repo.get_setting(db, "udp_server_ip", "")
            or ""
        ).strip()
        raw_server_port = repo.get_setting(
            db,
            "udp_server_port",
            None,
        )

        if not server_ip or raw_server_port in (None, ""):
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "where": "UDP_BATCH_TRANSFER_SKIPPED",
                    "batch_id": int(batch.id),
                    "reason": "udp_server_not_configured",
                },
            )
            return

        data = dict(batch.data or {})
        measured_good, measured_bad = _get_counts(batch)

        try:
            result = send_batch_data(
                server_ip=server_ip,
                server_port=int(raw_server_port),
                passport_number=data.get("passport_number"),
                passport_date=data.get("passport_date"),
                product_name=data.get("product_name"),
                product_code=data.get("product_code"),
                product_count=data.get("product_count"),
                measured_good=measured_good,
                measured_bad=measured_bad,
            )

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "where": "UDP_BATCH_TRANSFER_SENT",
                    "batch_id": int(batch.id),
                    "server_ip": server_ip,
                    "server_port": int(raw_server_port),
                    "message_id": result.message_id,
                    "packet_count": int(result.packet_count),
                    "bytes_sent": int(result.bytes_sent),
                    "payload": result.logical_payload,
                },
            )

        except Exception as exc:
            repo.add_event(
                db,
                severity=Severity.warn.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "where": "UDP_BATCH_TRANSFER_FAILED",
                    "batch_id": int(batch.id),
                    "server_ip": server_ip,
                    "server_port": raw_server_port,
                    "err": str(exc),
                },
            )


    def _im_connected_from_settings(self, db: Session) -> bool:
        v = repo.get_setting(db, "im_connected", True)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y", "on")
    

    def _im_settle_remaining_sec(self, db: Session) -> int:
        try:
            ready_at = float(repo.get_setting(db, "im_ready_at_ts", 0.0) or 0.0)
        except Exception:
            ready_at = 0.0

        left = ready_at - float(self.clock())

        if left <= 0:
            return 0

        return int(left + 0.999)


    def _im_ready_from_settings(self, db: Session) -> bool:
        return self._im_connected_from_settings(db) and self._im_settle_remaining_sec(db) <= 0

    
    def _im_error_from_settings(self, db: Session) -> str:
        return str(repo.get_setting(db, "im_error", "") or "").strip()
    
    
    def _set_im_wait_message_once(
        self,
        db: Session,
        message: str,
        *,
        event_key: str | None = None,
    ):
        message_changed = self._last_im_wait_message != message
        current_event_key = str(event_key or message)
        event_changed = self._last_im_wait_event_key != current_event_key
        message_severity = (
            Severity.warn
            if current_event_key.endswith(".settling")
            else Severity.error
        )

        if not message_changed and not event_changed:
            return

        # Точный текст (включая обратный отсчёт стабилизации)
        # обновляем в интерфейсе. Событие пишем только при переходе
        # между смысловыми состояниями: offline / settling.
        if message_changed:
            _set_message(
                db,
                message,
                message_severity,
                message_key="equipment.im",
            )
            self._last_im_wait_message = message

        if event_changed:
            repo.add_event(
                db,
                severity=message_severity.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": message,
                    "message_key": "equipment.im",
                    "where": "WAIT_IM_CONNECTION",
                    "state": current_event_key,
                },
            )
            self._last_im_wait_event_key = current_event_key


    def _setting_bool(self, db: Session, name: str, default: bool = False) -> bool:
        v = repo.get_setting(db, name, default)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)

        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off", "", "none", "null"):
            return False

        return bool(default)


    def _equipment_ready_for_auto_start(self, db: Session) -> tuple[bool, str | None]:
        reasons: list[str] = []

        if (not self._io_connected()) or (
            not self._setting_bool(db, "postamat_connected", self._io_connected())
        ):
            reasons.append("Постаматы")

        snap = self.rtk.snapshot()
        rtk_connected = bool(getattr(snap, "connected", False))
        if (not rtk_connected) or (
            not self._setting_bool(db, "rtk_connected", rtk_connected)
        ):
            reasons.append("РТК")

        if rtk_connected and not self._air_pressure_ok_from_snapshot(snap):
            reasons.append("Воздух")

        im_settle_left = self._im_settle_remaining_sec(db)

        if not self._setting_bool(db, "im_connected", True):
            reasons.append("Микрометр")
        elif im_settle_left > 0:
            reasons.append(f"Микрометр: стабилизация после восстановления ({im_settle_left} сек.)")

        if reasons:
            return False, "Старт невозможен: нет готовности оборудования: " + ", ".join(reasons)

        return True, None


    def _im_program_targets_for_batch(self, batch: BatchRow | None) -> tuple[str, str, int, str, str]:
        d = (batch.data or {}) if batch else {}

        raw_code = d.get("product_code") or d.get("product_name") or ""
        pn_base, pn_spec_from_code = parse_product_name_and_spec(str(raw_code))

        try:
            pn_spec = int(d.get("product_spec") or pn_spec_from_code or 0)
        except Exception:
            pn_spec = 0

        pn_full = str(raw_code).strip() or pn_base
        if pn_spec and "-" not in str(raw_code):
            pn_full = f"{pn_base}-{pn_spec:02d}"

        # Измерение:
        # - базовая деталь: полная программа == базовая;
        # - исполнение: программа с исполнением.
        measure_program = str(pn_full or pn_base or "").strip()

        # Калибровка/проверка:
        # - базовая деталь: базовая/полная программа;
        # - исполнение: базовая программа.
        calib_program = str((pn_base if pn_spec != 0 else pn_full) or pn_base or "").strip()

        return calib_program, measure_program, int(pn_spec), str(pn_base or ""), str(pn_full or "")



    # ---------- tick loop ----------

    def tick(self, db: Session):
        now = self.clock()
        self._refresh_runtime_settings(db)

        # --- IO snapshot (OPCUA + local) ---
        postamat_connected = self._io_connected()
        postamat_error = self._io_error()
        repo.set_setting(db, "postamat_connected", postamat_connected)
        repo.set_setting(db, "postamat_error", postamat_error or "")
        
        if postamat_connected:
            safety_hw = self.io.read_safety_ok()
            trash_present = self.io.read_trashcan_present()
        else:
            # fail-safe: если нет связи с постаматами, аварийный контур недоступен
            safety_hw = False
            trash_present = False
        
        safety_ok = safety_hw

        safety_status_code, safety_status_info = self._read_safety_status()
        if self._last_safety_status_code != safety_status_code:
            repo.set_setting(db, "safety_status_code", "" if safety_status_code is None else int(safety_status_code))
            repo.set_setting(db, "safety_status_info", safety_status_info or "")
            self._last_safety_status_code = safety_status_code        

        # --- temperature snapshot (float) ---
        t_load = self.io.read_temperature_sensor_loading()
        t_im = self.io.read_temperature_sensor_im()
        self._temp_last_loading = t_load
        self._temp_last_im = t_im

        temperature_state = self._build_temperature_state(
            t_load,
            t_im,
        )
        self._publish_temperature_snapshot(
            now=now,
            temperature_state=temperature_state,
        )
    
        repo.set_state(
            db,
            safety_ok=1 if safety_ok else 0,
            trash_present=1 if trash_present else 0,
            reject_bin_capacity=self.reject_bin_capacity,
        )
    
        # reject_count из БД
        st = repo.ensure_state_row(db)
        reject_count = int(st.reject_bin_count or 0)
        
        # --- RTK snapshot ---
        snap = self.rtk.snapshot()

        air_pressure_raw_ok = self._air_pressure_ok_from_snapshot(snap)

        if air_pressure_raw_ok:
            self._air_pressure_lost_since = None
            air_pressure_ok = True
        else:
            if self._air_pressure_lost_since is None:
                self._air_pressure_lost_since = now

            no_air_confirmed = (
                (now - float(self._air_pressure_lost_since))
                >= float(self._air_pressure_lost_debounce_sec)
            )

            # Короткий провал DI09 на старте/переключении RTK не должен
            # создавать paused_no_air, сообщение "нажмите ПРОДОЛЖИТЬ"
            # и аварийную свето-звуковую индикацию.
            if (
                no_air_confirmed
                or self._last_air_pressure_ok is False
                or self._air_pressure_emergency_active
            ):
                air_pressure_ok = False
            else:
                air_pressure_ok = True

        repo.set_setting(db, "air_pressure_ok", bool(air_pressure_ok))

        c1 = getattr(snap, "connected_r1", None)
        c2 = getattr(snap, "connected_r2", None)
        if c1 is None: c1 = snap.connected
        if c2 is None: c2 = snap.connected

        # Отдельные состояния подключения роботов для интерфейса оператора.
        # r1 = RS013N, r2 = RS007L.
        repo.set_setting(
            db,
            "rtk_connected_r1",
            bool(c1),
        )
        repo.set_setting(
            db,
            "rtk_connected_r2",
            bool(c2),
        )        
        
        cs1 = getattr(snap, "cs_r1", None)
        cs2 = getattr(snap, "cs_r2", None)
        
        need_cycle_on = bool(c1 and c2 and ((cs1 is False) or (cs2 is False)))

        # Публикуем флаг для кнопки оператора.
        if self._last_need_cycle_on is None or self._last_need_cycle_on != need_cycle_on:
            repo.set_setting(db, "rtk_need_cycle_on", need_cycle_on)
            self._last_need_cycle_on = need_cycle_on


        a1 = str(getattr(snap, "action_r1", "") or "").strip().lower()
        a2 = str(getattr(snap, "action_r2", "") or "").strip().lower()
        repo.set_setting(db, "rtk_connected", bool(snap.connected))
        repo.set_setting(db, "rtk_error", snap.error or "")

        repo.set_state(
            db,
            rtk_connected=1 if snap.connected else 0,
            rtk_busy=1 if snap.busy else 0,
            rtk_state=f"r1:{a1 or '-'} r2:{a2 or '-'}",
            rtk_action_r1=f"rs013n:{a1 or '-'}",
            rtk_action_r2=f"rs007l:{a2 or '-'}",
            rtk_error=snap.error,
            rtk_pickcount=snap.pickcount,
            rtk_defectcount=snap.defectcount,
        )

        # Длительные предупреждения синхронизируем до любых ранних return.
        # Это не даёт им зависнуть или исчезнуть при одновременной аварии.
        positioner_missing = self._sync_positioner_missing_message(
            db,
            snap=snap,
            a1=a1,
            a2=a2,
        )
        self._sync_blocked_batch_door_messages(db)

        if a2 not in {"waitingmmresult", "waitingcalibrationresult"}:
            blocked_batches = db.execute(
                select(BatchRow).where(BatchRow.status == "rejected")
            ).scalars().all()

            for b_blk in blocked_batches:
                d_blk = dict(b_blk.data or {})
                if not bool(d_blk.get("extraction_wait_rtk_safe")):
                    continue

                d_blk["extraction_wait_rtk_safe"] = False
                d_blk["extraction_blocked_rtk_state"] = None
                b_blk.data = d_blk
                flag_modified(b_blk, "data")

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "batch extraction unblocked: R2 left external wait state",
                        "batch_id": int(b_blk.id),
                        "r2_action": a2,
                    },
                )        
    
        # --- RTK waiting gates (handshake) ---
        # measurement_result: only when rs007l.action == "WaitingMMResult"
        # calibration_result: only when rs007l.action == "WaitingCalibrationResult"
        st = repo.ensure_state_row(db)
        reject_count = int(st.reject_bin_count or 0)

        # --- per-tare tracking for labels/protocol (snap.tareout/snap.putcount) ---
        try:
            bid_tt = int(st.active_batch_id or 0)
        except Exception:
            bid_tt = 0

        tareout_raw = getattr(snap, "tareout", None)
        putcount_raw = getattr(snap, "putcount", None)

        # Временные paused_* режимы tracking не сбрасывают.
        # Сброс выполняется только на окончательной границе партии.
        if bid_tt <= 0:
            self._reset_tare_tracking(
                reason="no_active_batch",
            )
        elif tareout_raw is not None and putcount_raw is not None:
            try:
                tareout_i = int(tareout_raw)
                putcount_i = int(putcount_raw)
            except (TypeError, ValueError):
                tareout_i = None
                putcount_i = None

            if tareout_i is not None and putcount_i is not None:
                phase_tt = str(
                    getattr(st, "active_batch_phase", "")
                    or ""
                ).strip().lower()
                operation_phase_tt = str(
                    getattr(st, "active_operation_phase", "")
                    or ""
                ).strip().lower()
                robots_left_waiting_tt = (
                    (bool(a1) and a1 != "waitingforstart")
                    or (bool(a2) and a2 != "waitingforstart")
                )
                cycle_started_tt = (
                    phase_tt in {
                        "running",
                        "finishing",
                        STOP_CLEANUP_PHASE,
                    }
                    or operation_phase_tt in {
                        "running",
                        "finishing",
                    }
                    or (
                        operation_phase_tt == "rtk_start_sent"
                        and robots_left_waiting_tt
                    )
                )

                # Новый batch_id означает новый tracking-контекст.
                if (
                    self._tare_track_batch_id not in (None, bid_tt)
                    or self._tare_pending_batch_id not in (None, bid_tt)
                ):
                    old_context_batch = (
                        self._tare_track_batch_id
                        if self._tare_track_batch_id is not None
                        else self._tare_pending_batch_id
                    )
                    self._reset_tare_tracking(
                        reason="active_batch_changed",
                        batch_id=old_context_batch,
                    )
                    self._tare_pending_batch_id = int(bid_tt)
                    self._tare_prestart_putcount = int(putcount_i)
                    self._tare_putcount_reset_seen = (
                        int(putcount_i) == 0
                    )

                # Восстановление tracking после рестарта daemon.
                if (
                    self._tare_track_batch_id is None
                    and self._tare_pending_batch_id is None
                ):
                    if cycle_started_tt:
                        b_recover = repo.get_batch(db, bid_tt)
                        recovered_tares = (
                            _norm_out_tares(b_recover)
                            if b_recover
                            else []
                        )
                        recovered_base = sum(
                            int(item.get("qty") or 0)
                            for item in recovered_tares
                        )
                        recovered_base = min(
                            max(0, int(recovered_base)),
                            max(0, int(putcount_i)),
                        )
                        self._tare_track_batch_id = int(bid_tt)
                        self._tare_last_tareout = int(tareout_i)
                        self._tare_last_putcount = int(putcount_i)
                        self._tare_putcount_base = int(recovered_base)
                        self._tare_indexing = (
                            (
                                "zero"
                                if int(tareout_i) == 0
                                else "one"
                            )
                            if int(putcount_i) > 0
                            else None
                        )
                    else:
                        self._tare_pending_batch_id = int(bid_tt)
                        self._tare_prestart_putcount = int(putcount_i)
                        self._tare_putcount_reset_seen = (
                            int(putcount_i) == 0
                        )

                if self._tare_pending_batch_id == bid_tt:
                    prestart_pc = self._tare_prestart_putcount

                    if (
                        not self._tare_putcount_reset_seen
                        and (
                            int(putcount_i) == 0
                            or (
                                prestart_pc is not None
                                and int(putcount_i) < int(prestart_pc)
                            )
                        )
                    ):
                        # Нулевой snapshot мог пройти между опросами,
                        # а первая деталь уже увеличить putcount.
                        self._tare_putcount_reset_seen = True

                    if cycle_started_tt and self._tare_putcount_reset_seen:
                        self._tare_track_batch_id = int(bid_tt)
                        self._tare_last_tareout = int(tareout_i)
                        self._tare_last_putcount = int(putcount_i)
                        self._tare_putcount_base = 0
                        self._tare_indexing = (
                            (
                                "zero"
                                if int(tareout_i) == 0
                                else "one"
                            )
                            if int(putcount_i) > 0
                            else None
                        )
                        self._tare_pending_batch_id = None
                        self._tare_prestart_putcount = None
                        self._tare_putcount_reset_seen = False

                tracking_active = (
                    self._tare_track_batch_id == bid_tt
                )

                if tracking_active:
                    last_tareout = int(self._tare_last_tareout)
                    last_pc = int(self._tare_last_putcount)
                    base_pc = int(self._tare_putcount_base)
                else:
                    # Без активного tracking старый snapshot не должен создавать или завершать тару
                    last_tareout = int(tareout_i)
                    last_pc = int(putcount_i)
                    base_pc = int(putcount_i)

                def _write_tare(
                    tare_no: int,
                    qty: int,
                ) -> bool:
                    if qty <= 0 or tare_no <= 0:
                        return False

                    b_tt = repo.get_batch(db, bid_tt)
                    if not b_tt:
                        return False

                    try:
                        db.refresh(b_tt)
                    except Exception:
                        pass

                    cell_no_i = _first_out_cell(b_tt)
                    if cell_no_i <= 0:
                        return False

                    _upsert_out_tare(
                        b_tt,
                        cell_no=cell_no_i,
                        tare_no=int(tare_no),
                        qty=int(qty),
                    )
                    db.commit()
                    return True

                # Определяем нумерацию тар только после первой детали.
                if (
                    tracking_active
                    and self._tare_indexing is None
                    and putcount_i > 0
                ):
                    self._tare_indexing = (
                        "zero" if tareout_i == 0 else "one"
                    )

                # 1) reset to 0/0 => финализируем последнюю тару.
                if (
                    tracking_active
                    and tareout_i == 0
                    and putcount_i == 0
                    and last_pc > 0
                ):
                    tare_no = self._tare_idx_to_no(last_tareout)
                    qty = last_pc - base_pc
                    if qty < 0:
                        qty = last_pc

                    _write_tare(tare_no, qty)
                    self._tare_last_tareout = 0
                    self._tare_last_putcount = 0
                    self._tare_putcount_base = 0
                    self._tare_indexing = None

                # 2) tare changed => предыдущая тара завершена.
                elif tracking_active and tareout_i != last_tareout:
                    tare_no = self._tare_idx_to_no(last_tareout)
                    qty = last_pc - base_pc
                    if qty < 0:
                        qty = last_pc

                    _write_tare(tare_no, qty)

                    # Граница новой тары проходит по предыдущему
                    # snapshot. Одновременный прирост putcount уже
                    # относится к первой детали новой тары.
                    self._tare_last_tareout = int(tareout_i)
                    self._tare_putcount_base = int(last_pc)
                    self._tare_last_putcount = int(putcount_i)

                # 3) та же тара: обновляем накопительный putcount.
                elif tracking_active:
                    if putcount_i > last_pc:
                        self._tare_last_putcount = int(putcount_i)
                    elif putcount_i < last_pc:
                        self._tare_putcount_base = int(putcount_i)
                        self._tare_last_putcount = int(putcount_i)

        # сброс антиспама, если вышли из ожидания результата
        if a2 != "waitingmmresult":
            self._mm_req_sent_batch = None

        if a2 == "waitingmmresult":
            bid = int(
                getattr(st, "active_operation_batch_id", None)
                or st.active_batch_id
                or 0
            ) or None
            operation_id = str(getattr(st, "active_operation_id", "") or "").strip()

            # R2 мог войти в waitingmmresult уже ПОСЛЕ принятия STOP:
            # в этот момент operation_id штатно очищен, а новую команду ИМ
            # запускать уже нельзя. Подставляем технический NOK, чтобы
            # закрыть gate РТК; физически деталь уйдёт в тару брака и
            # затем будет учтена _sync_stop_cleanup_unmeasured().
            stop_cleanup_mm_gate = (
                str(st.active_batch_phase or "")
                == STOP_CLEANUP_PHASE
            )

            if (
                stop_cleanup_mm_gate
                and st.pending_mm_result is None
                and not bool(st.mm_result_inflight)
            ):
                repo.set_state(
                    db,
                    pending_mm_result=0,
                    mm_result_inflight=0,
                )
                st = repo.ensure_state_row(db)

            if (
                bid
                and not operation_id
                and not stop_cleanup_mm_gate
            ):
                _set_message(
                    db,
                    "ОШИБКА: нет active_operation_id для текущего ожидания результата измерения",
                    Severity.error,
                    message_key=(
                        _batch_operator_message_key(
                            IM_WORKFLOW_MESSAGE_PREFIX,
                            bid,
                        )
                        or IM_WORKFLOW_MESSAGE_PREFIX
                    ),
                )
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "WAITING_MM_RESULT",
                        "err": "missing_active_operation_id",
                        "batch_id": bid,
                    },
                )
                return

            im_online = self._im_connected_from_settings(db)
            im_settle_left = self._im_settle_remaining_sec(db)
            im_ready = bool(im_online and im_settle_left <= 0)
            has_pending_result = st.pending_mm_result is not None

            # Если РТК ждёт результат измерения, а ИМ offline и результата ещё нет:
            # - НЕ отправляем в РТК синтетический/пустой результат;
            # - НЕ считаем попытку измерения "навсегда отправленной";
            # - ждём восстановления связи, после чего этот же блок заново поставит IM_MEASURE_ONCE.
            if (not im_ready) and (not has_pending_result):
                if not im_online:
                    err = self._im_error_from_settings(db)
                    msg = "Микрометр: нет связи; РТК ждёт результат измерения"
                    if err:
                        msg += f" ({err})"
                else:
                    msg = (
                        "Микрометр: связь восстановлена; "
                        f"ожидание стабилизации {im_settle_left} сек.; "
                        "РТК ждёт результат измерения"
                    )

                self._set_im_wait_message_once(
                    db,
                    msg,
                    event_key=(
                        "measurement.offline"
                        if not im_online
                        else "measurement.settling"
                    ),
                )

                self._mm_req_sent_batch = None
                repo.set_state(db, mm_result_inflight=0)

            else:
                if im_ready:
                    self._last_im_wait_message = None
                    self._last_im_wait_event_key = None

                # 0) если уже добили количество - новое измерение не запускаем,
                # но если pending_mm_result уже есть, его ниже всё равно отправим в РТК.
                stop_new_mm = False
                if bid:
                    b = repo.get_batch(db, int(bid))
                    measured = int((b.data or {}).get("measured_qty") or 0) if b else 0
                    expected = int(st.active_batch_expected_count or 0)
                    stop_new_mm = (expected > 0 and measured >= expected)

                # 1) если результата ещё нет - запрашиваем измерение.
                # Антидубль _mm_req_sent_batch нужен только чтобы не плодить pending-команды
                # пока первая ещё не обработана.
                # Если IM-команда упадёт из-за offline, main.py должен сбросить этот флаг.
                if (
                    (not stop_new_mm)
                    and bid
                    and st.pending_mm_result is None
                    and not bool(st.mm_result_inflight)
                    and self._mm_req_sent_batch != bid
                ):
                    b = repo.get_batch(db, bid)
                    _, measure_program, _, _, _ = self._im_program_targets_for_batch(b)
                    pc = measure_program or (getattr(b, "product_code", None) if b else None)

                    if pc:
                        loaded_im_program = str(repo.get_setting(db, "im_loaded_program", "") or "").strip()
                        need_measure_program_load = loaded_im_program != str(pc).strip()

                        # После offline/зависания ИМ программа могла сброситься.
                        # Поэтому перед измерением сначала грузим целевую программу.
                        if need_measure_program_load:
                            db.add(CommandRow(
                                type=CommandType.IM_LOAD_PROGRAM.value,
                                status=CommandStatus.pending.value,
                                payload={
                                    "product_code": str(pc),
                                    "batch_id": int(bid),
                                    "operation_id": operation_id,                                    
                                    "phase": "pre_measurement",
                                    "reason": "waiting_mm_result",
                                },
                                created_by="DAEMON",
                            ))

                        db.add(CommandRow(
                            type=CommandType.IM_MEASURE_ONCE.value,
                            status=CommandStatus.pending.value,
                            payload={
                                "product_code": str(pc),
                                "batch_id": int(bid),
                                "operation_id": operation_id,                                
                            },
                            created_by="DAEMON",
                        ))
                        db.commit()
                        self._mm_req_sent_batch = bid

                        repo.add_event(
                            db,
                            severity=Severity.info.value,
                            source="DAEMON",
                            type_=EventType.STATE_UPDATED.value,
                            payload={
                                "message": "IM_MEASURE_ONCE queued from WaitingMMResult",
                                "batch_id": int(bid),
                                "operation_id": operation_id,                                
                                "product_code": str(pc),
                                "program_load_queued": bool(need_measure_program_load),
                                "loaded_im_program": loaded_im_program,                                
                            },
                        )
                    else:
                        _set_message(
                            db,
                            "Микрометр: невозможно запустить измерение - отсутствует обозначение детали для партии",
                            Severity.error,
                            message_key=(
                                _batch_operator_message_key(
                                    IM_WORKFLOW_MESSAGE_PREFIX,
                                    bid,
                                )
                                or IM_WORKFLOW_MESSAGE_PREFIX
                            ),
                        )
                        repo.add_event(
                            db,
                            severity=Severity.error.value,
                            source="DAEMON",
                            type_=EventType.ERROR.value,
                            payload={
                                "where": "WAITING_MM_RESULT",
                                "err": "product_code is empty",
                                "batch_id": bid,
                            },
                        )

                # 2) если результат уже есть - отправляем в RTK.
                # Это можно делать даже если ИМ уже успела отвалиться:
                # результат уже записан в state и должен закрыть gate РТК.
                st = repo.ensure_state_row(db)

                if st.pending_mm_result is not None and not bool(st.mm_result_inflight):
                    self.rtk.request(
                        "send_measurement_result",
                        {"result": bool(st.pending_mm_result)},
                    )

                    # Сообщение результата ИМ снимаем именно после
                    # успешной постановки результата в очередь РТК.
                    # Следующий RTK_COMMAND_SENT event сразу обновит UI.
                    _clear_batch_operator_messages(
                        db,
                        bid,
                        IM_WORKFLOW_MESSAGE_PREFIX,
                    )

                    # --- schedule calibration after N consecutive NOK (from IM),
                    # after sending NOK to RTK ---
                    mm_ok = bool(st.pending_mm_result)
                    is_reject = not mm_ok

                    # consecutive_rejects может лежать в state или в batch.data
                    consec = int(getattr(st, "consecutive_rejects", 0) or 0)
                    if consec <= 0 and bid:
                        bb = repo.get_batch(db, int(bid))
                        consec = int((bb.data or {}).get("consecutive_rejects") or 0) if bb else 0

                    reject_threshold = int(
                        self._consecutive_rejects_threshold
                    )

                    if (
                        is_reject
                        and consec >= reject_threshold
                        and not stop_cleanup_mm_gate                        
                        and (not bool(getattr(st, "calibration_inflight", 0)))
                        and self._calib_due_at is None
                        and not self._deferred_stop_active(
                            db,
                            batch_id=bid,
                            operation_id=operation_id,
                        )
                    ):
                        self._calib_due_at = now + 0.5
                        self._calib_due_batch = int(bid) if bid else None
                        self._calib_due_operation_id = operation_id
                        self._calib_due_reject_threshold = reject_threshold

                        _set_message(
                            db,
                            f"Критерий брака: достигнут порог подряд идущих "
                            f"браков ({reject_threshold}); запрошена проверка",
                            Severity.warn,
                            message_key=(
                                _batch_operator_message_key(
                                    QUALITY_CALIBRATION_MESSAGE_PREFIX,
                                    bid,
                                )
                                or QUALITY_CALIBRATION_MESSAGE_PREFIX
                            ),
                        )
                        repo.add_event(
                            db,
                            severity=Severity.warn.value,
                            source="DAEMON",
                            type_=EventType.CALIBRATION_REQUESTED.value,
                            payload={
                                "where": "SCHEDULE_AFTER_3_REJECTS",
                                "batch_id": self._calib_due_batch,
                                "operation_id": self._calib_due_operation_id,                                
                                "due_in_ms": 500,
                                "consecutive_rejects_threshold": reject_threshold,
                            },
                        )

                    repo.add_event(
                        db,
                        severity=Severity.info.value,
                        source="DAEMON",
                        type_=EventType.RTK_COMMAND_SENT.value,
                        payload={
                            "cmd": "send_measurement_result",
                            "result": bool(st.pending_mm_result),
                            "batch_id": bid,
                            "operation_id": operation_id,
                            "synthetic": bool(stop_cleanup_mm_gate),
                            "reason": (
                                "stop_cleanup"
                                if stop_cleanup_mm_gate
                                else None
                            ),                                                        
                        },
                    )
                    repo.set_state(
                        db,
                        pending_mm_result=None,
                        mm_result_inflight=1,
                    )

        else:
            # если вышли из ожидания - считаем, что RTK принял результат
            if bool(st.mm_result_inflight):
                repo.set_state(db, mm_result_inflight=0)


        # сброс антиспама, если вышли из ожидания калибровки
        if a2 != "waitingcalibrationresult":
            self._calib_req_sent_batch = None
            self._calib_program_load_req_key = None
            self._check_to_calibration_rearm_at = None
            self._check_to_calibration_rearm_key = None
        
        calib_gate_bid = int(
            getattr(st, "active_operation_batch_id", None)
            or st.active_batch_id
            or 0
        ) or None
        calib_gate_operation_id = str(
            getattr(st, "active_operation_id", "") or ""
        ).strip()

        # После фатального -2/-4 _rtk_stop может обрабатываться РТК не мгновенно,
        # Аналогичная гонка возможна с калибровкой/проверкой:
        # STOP уже принят, но R2 только после этого доходит до
        # waitingcalibrationresult. Реальную операцию ИМ больше не запускаем;
        # технический 0 нужен только для закрытия gate РТК.
        stop_cleanup_calib_gate = (
            a2 == "waitingcalibrationresult"
            and str(st.active_batch_phase or "")
            == STOP_CLEANUP_PHASE
        )

        # После -3 проверка переводится в калибровку через
        # send_calibration_result(-3) -> resume. На некоторых циклах РТК
        # action_r2 всё это время остаётся waitingcalibrationresult, поэтому
        # обычный edge-сброс ниже не наступает и старый inflight блокирует
        # новую команду IM_CALIBRATE.
        #
        # Если edge виден, используется прежний путь без задержки. Этот
        # fallback срабатывает только при непрерывном waiting-состоянии и
        # выдерживает защитную паузу для установки эталона роботом.
        gate_reason = str(
            getattr(st, "calibration_reason", "")
            or ""
        ).lower()
        check_to_calibration_pending = bool(
            a2 == "waitingcalibrationresult"
            and bool(getattr(st, "calib_result_inflight", 0))
            and "retry_mode=1" in gate_reason
            and CHECK_TO_CALIBRATION_MARKER in gate_reason
            and not bool(getattr(st, "rtk_paused", 0))
            and not stop_cleanup_calib_gate
            and calib_gate_bid
            and calib_gate_operation_id
        )

        if check_to_calibration_pending:
            rearm_key = (
                int(calib_gate_bid),
                str(calib_gate_operation_id),
            )

            if self._check_to_calibration_rearm_key != rearm_key:
                self._check_to_calibration_rearm_key = rearm_key
                self._check_to_calibration_rearm_at = (
                    float(now)
                    + float(CHECK_TO_CALIBRATION_REARM_SEC)
                )

            elif (
                self._check_to_calibration_rearm_at is not None
                and float(now)
                >= float(self._check_to_calibration_rearm_at)
            ):
                rearmed_reason = re.sub(
                    rf"\s*;?\s*{re.escape(CHECK_TO_CALIBRATION_MARKER)}\s*",
                    "",
                    str(getattr(st, "calibration_reason", "") or ""),
                ).strip(" ;")
                repo.set_state(
                    db,
                    calib_result_inflight=0,
                    calibration_inflight=0,
                    calibration_reason=rearmed_reason or None,
                )
                self._calib_req_sent_batch = None
                self._calib_program_load_req_key = None
                self._check_to_calibration_rearm_at = None
                self._check_to_calibration_rearm_key = None
                st = repo.ensure_state_row(db)

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": (
                            "continuous waitingcalibrationresult re-armed "
                            "for calibration after failed check"
                        ),
                        "batch_id": int(calib_gate_bid),
                        "operation_id": str(calib_gate_operation_id),
                        "delay_sec": float(
                            CHECK_TO_CALIBRATION_REARM_SEC
                        ),
                    },
                )
        else:
            self._check_to_calibration_rearm_at = None
            self._check_to_calibration_rearm_key = None

        if (
            stop_cleanup_calib_gate
            and st.pending_calib_result is None
            and not bool(st.calib_result_inflight)
        ):
            repo.set_state(
                db,
                pending_calib_result=0,
                calib_result_inflight=0,
                calibration_inflight=0,
                calibration_reason=None,
                calib_wait_im_program=0,
                calib_im_program_target=None,
            )
            st = repo.ensure_state_row(db)

        # R2 ещё несколько tick остаётся в waitingcalibrationresult.
        # Активная операция к этому моменту уже штатно завершена - не запускаем
        # новую калибровку без контекста и не спамим missing_active_operation_id.
        if (
            a2 == "waitingcalibrationresult"
            and not calib_gate_bid
            and not calib_gate_operation_id
            and not stop_cleanup_calib_gate            
        ):
            self._calib_req_sent_batch = None
            self._calib_program_load_req_key = None
            if (
                st.pending_calib_result is not None
                or bool(st.calib_result_inflight)
                or bool(st.calibration_inflight)
            ):
                repo.set_state(
                    db,
                    pending_calib_result=None,
                    calib_result_inflight=0,
                    calibration_inflight=0,
                    calibration_reason=None,
                    calib_wait_im_program=0,
                    calib_im_program_target=None,
                )

        elif a2 == "waitingcalibrationresult":
            bid = calib_gate_bid
            operation_id = calib_gate_operation_id

            if (
                bid
                and not operation_id
                and not stop_cleanup_calib_gate
            ):
                _set_message(
                    db,
                    "ОШИБКА: нет active_operation_id для текущей калибровки/проверки",
                    Severity.error,
                    message_key=(
                        _batch_operator_message_key(
                            IM_WORKFLOW_MESSAGE_PREFIX,
                            bid,
                        )
                        or IM_WORKFLOW_MESSAGE_PREFIX
                    ),
                )
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "WAITING_CALIBRATION_RESULT",
                        "err": "missing_active_operation_id",
                        "batch_id": bid,
                    },
                )
                return

            im_online = self._im_connected_from_settings(db)
            im_settle_left = self._im_settle_remaining_sec(db)
            im_ready = bool(im_online and im_settle_left <= 0)

            if not im_ready and not stop_cleanup_calib_gate:
                if not im_online:
                    err = self._im_error_from_settings(db)
                    msg = "Микрометр: нет связи; РТК ждёт результат калибровки/проверки"
                    if err:
                        msg += f" ({err})"
                else:
                    msg = (
                        "Микрометр: связь восстановлена; "
                        f"ожидание стабилизации {im_settle_left} сек.; "
                        "РТК ждёт результат калибровки/проверки"
                    )

                self._set_im_wait_message_once(
                    db,
                    msg,
                    event_key=(
                        "calibration.offline"
                        if not im_online
                        else "calibration.settling"
                    ),
                )

                # Важно: сбрасываем антиспам, чтобы после восстановления связи
                # снова поставить IM_LOAD_PROGRAM / IM_CALIBRATE.
                self._calib_req_sent_batch = None
                self._calib_program_load_req_key = None                
                repo.set_state(
                    db,
                    calib_result_inflight=0,
                    calibration_inflight=0,
                )
                return
            else:
                self._last_im_wait_message = None
                self._last_im_wait_event_key = None

            b0 = repo.get_batch(db, int(bid)) if bid else None
            calib_program, measure_program, pn_spec, pn_base, pn_full = self._im_program_targets_for_batch(b0)
 
            # Если калибровка уже завершилась, но после неё надо вернуть программу измерения,
            # а загрузка сорвалась из-за offline - повторяем загрузку до отправки результата в РТК.
            st = repo.ensure_state_row(db)

            if st.pending_calib_result is not None and bool(getattr(st, "calib_wait_im_program", 0)):
                target_after_calib = str(
                    getattr(st, "calib_im_program_target", None)
                    or measure_program
                    or ""
                ).strip()

                loaded_im_program = str(repo.get_setting(db, "im_loaded_program", "") or "").strip()

                if target_after_calib and loaded_im_program == target_after_calib:
                    repo.set_state(
                        db,
                        calib_wait_im_program=0,
                        calib_im_program_target=None,
                    )
                    self._calib_program_load_req_key = None

                elif target_after_calib:
                    load_key = f"{int(bid or -1)}:after_calibration:{target_after_calib}"

                    if self._calib_program_load_req_key != load_key:
                        db.add(CommandRow(
                            type=CommandType.IM_LOAD_PROGRAM.value,
                            status=CommandStatus.pending.value,
                            payload={
                                "product_code": target_after_calib,
                                "batch_id": bid,
                                "operation_id": operation_id,                                
                                "after_calibration": 1,
                                "phase": "after_calibration_retry",
                                "delay_before_load_sec": 3.0,
                            },
                            created_by="DAEMON",
                        ))
                        db.commit()
                        self._calib_program_load_req_key = load_key

            # 1) Если результата ещё нет - готовим программу и запускаем
            # ровно одну IM_CALIBRATE.
            #
            # IM_LOAD_PROGRAM и IM_CALIBRATE нельзя ставить в очередь
            # одновременно. Сначала подтверждаем загрузку программы,
            # только затем запускаем калибровку/проверку.
            calibration_retry_safety_pending = (
                self._calibration_retry_safety_gate_matches(
                    db,
                    batch_id=bid,
                    operation_id=operation_id,
                )
            )

            if (
                st.pending_calib_result is None
                and not bool(st.calib_result_inflight)
                and not bool(st.calibration_inflight)
                and not bool(getattr(st, "rtk_paused", 0))
                and not calibration_retry_safety_pending
            ):
                reason = (st.calibration_reason or "").strip().lower()

                if CHECK_TO_CALIBRATION_MARKER in reason:
                    reason = re.sub(
                        rf"\s*;?\s*{re.escape(CHECK_TO_CALIBRATION_MARKER)}\s*",
                        "",
                        reason,
                    ).strip(" ;")
                    repo.set_state(
                        db,
                        calibration_reason=reason or None,
                    )
                    st = repo.ensure_state_row(db)

                # IM: 1 = calibration, 2 = check.
                # Для повторной операции после -1/-3 используем явный marker.
                if "retry_mode=2" in reason:
                    mode = 2
                elif "retry_mode=1" in reason:
                    mode = 1
                else:
                    mode = (
                        2
                        if ("three_consecutive" in reason or "3" in reason)
                        else 1
                    )

                loaded_im_program = str(
                    repo.get_setting(
                        db,
                        "im_loaded_program",
                        "",
                    )
                    or ""
                ).strip()

                need_pre_calib_load = bool(
                    calib_program
                    and loaded_im_program != str(calib_program)
                )

                if need_pre_calib_load:
                    # Сначала ставим только загрузку программы.
                    # IM_CALIBRATE будет поставлена на следующем tick
                    # после подтверждения settings.im_loaded_program.
                    load_key = (
                        f"{int(bid or -1)}:pre_calibration:"
                        f"{str(calib_program)}:mode={int(mode)}"
                    )

                    if self._calib_program_load_req_key != load_key:
                        db.add(
                            CommandRow(
                                type=CommandType.IM_LOAD_PROGRAM.value,
                                status=CommandStatus.pending.value,
                                payload={
                                    "product_code": str(calib_program),
                                    "batch_id": bid,
                                    "operation_id": operation_id,
                                    "phase": "pre_calibration",
                                    "settle_after_load_sec": 6.0,
                                    "next_calib_mode": int(mode),
                                    "reason": st.calibration_reason,
                                },
                                created_by="DAEMON",
                            )
                        )
                        db.commit()
                        self._calib_program_load_req_key = load_key

                else:
                    # Программа подтверждена - теперь запускаем калибровку.
                    self._calib_program_load_req_key = None

                    if self._calib_req_sent_batch != (bid or -1):
                        db.add(
                            CommandRow(
                                type=CommandType.IM_CALIBRATE.value,
                                status=CommandStatus.pending.value,
                                payload={
                                    "batch_id": bid,
                                    "operation_id": operation_id,
                                    "pulse": int(mode),
                                    "reason": st.calibration_reason,
                                    "required_program": str(calib_program),
                                    "required_program_settle_after_load_sec": 6.0,
                                },
                                created_by="DAEMON",
                            )
                        )

                        # Сохраняемый в БД latch дополнительно защищает
                        # от повторной постановки IM_CALIBRATE.
                        repo.set_state(
                            db,
                            calibration_inflight=1,
                        )

                        db.commit()
                        self._calib_req_sent_batch = (bid or -1)
        
            # 2) если результат уже есть - отправляем на РТК
            if (
                st.pending_calib_result is not None
                and not bool(st.calib_result_inflight)
                and not bool(getattr(st, "calib_wait_im_program", 0))
            ):
                sent_calib_result = int(st.pending_calib_result)


                self.rtk.request(
                    "send_calibration_result",
                    {"result": sent_calib_result},
                )


                if sent_calib_result == 0:
                    # Успешное сообщение ИМ снимается сразу после
                    # постановки результата в очередь РТК. Событие ниже
                    # гарантирует немедленное обновление workplace.
                    _clear_batch_operator_messages(
                        db,
                        bid,
                        QUALITY_CALIBRATION_MESSAGE_PREFIX,
                        IM_WORKFLOW_MESSAGE_PREFIX,
                    )

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.RTK_COMMAND_SENT.value,
                    payload={
                        "cmd": "send_calibration_result",
                        "result": sent_calib_result,
                        "batch_id": bid,
                        "operation_id": operation_id,
                        "synthetic": bool(stop_cleanup_calib_gate),
                        "reason": (
                            "stop_cleanup"
                            if stop_cleanup_calib_gate
                            else None
                        ),                                                
                    }
                )

                state_patch = {
                    "pending_calib_result": None,
                    "calib_result_inflight": 1,
                    "calibration_inflight": 0,
                    # Для -1/-3 сохраняем retry_mode marker до успешного повтора.
                    "calibration_reason": (
                        st.calibration_reason
                        if sent_calib_result in (-1, -3)
                        else None
                    ),
                    "consecutive_rejects": 0,
                    "calib_wait_im_program": 0,
                    "calib_im_program_target": None,
                }

                if sent_calib_result == -1:
                    # После -1 РТК сам перейдёт в paused.
                    # Локальный флаг удерживает gate до осмотра
                    # и ручного RESUME_SYSTEM.
                    state_patch.update(
                        rtk_paused=1,
                        rtk_pause_reason="calibration_retry_required",
                    )

                elif sent_calib_result == -3:
                    # Для -3 оператор в ячейку не входит. Не используем
                    # _rtk_resume(): он идемпотентно возвращается при
                    # рассинхронизированном локальном rtk_paused. Команду
                    # resume ставим напрямую сразу после результата, поэтому
                    # FIFO гарантирует порядок result(-3) -> resume.
                    state_patch.update(
                        rtk_paused=0,
                        rtk_pause_reason=None,
                    )

                repo.set_state(db, **state_patch)

                if sent_calib_result == -3:
                    self.rtk.request(
                        "resume",
                        {
                            "reason": (
                                "check_failed_start_calibration"
                            )
                        },
                    )
                    repo.add_event(
                        db,
                        severity=Severity.info.value,
                        source="DAEMON",
                        type_=EventType.RTK_COMMAND_SENT.value,
                        payload={
                            "cmd": "resume",
                            "reason": (
                                "check_failed_start_calibration"
                            ),
                            "batch_id": bid,
                            "operation_id": operation_id,
                        },
                    )
        else:
            # Вышли из gate — считаем, что РТК принял результат.
            # Сообщение успешной операции уже снято в момент request();
            # здесь меняем только handshake-состояние.
            if bool(st.calib_result_inflight):
                repo.set_state(db, calib_result_inflight=0)

        # Если STOP_AUTO был нажат, пока R2 ждал результат ИМ/калибровки,
        # завершаем остановку только после выхода R2 из waitingmmresult/waitingcalibrationresult.
        if self._maybe_finish_deferred_stop_after_external_result(db, a2=a2):
            return


        # R1 работа с тарой ОП и ОПТ
        # если вышли из wait* - сбрасываем только текущую попытку
        if a1 != "waitinstockersensor":
            self._wait_in_attempt_id = None
            self._wait_in_attempt_seen_at = 0.0
        if a1 != "waitoutstockersensor":
            self._wait_out_attempt_id = None
            self._wait_out_attempt_seen_at = 0.0

        if snap.connected and st.active_batch_id is not None:
            b = repo.get_batch(db, int(st.active_batch_id))
            if b:
                data = b.data or {}
                loc = b.location or {}

                in_ids = data.get("in_tare_ids") or loc.get("in_tare_ids") or []
                out_ids = data.get("out_tare_ids") or loc.get("out_tare_ids") or []

                if not isinstance(in_ids, list):
                    in_ids = [in_ids]
                if not isinstance(out_ids, list):
                    out_ids = [out_ids]

                try:
                    in_ids = [int(x) for x in in_ids]
                    out_ids = [int(x) for x in out_ids]
                except Exception:
                    in_ids, out_ids = [], []

                def cell_to_col(cell: int, col_count: int) -> int:
                    return ((int(cell) - 1) % int(col_count)) + 1

                # загрузка: 15 ячеек => 3 колонны
                if a1 == "waitinstockersensor" and in_ids:

                    # reset per-batch attempt memory
                    if self._wait_in_batch_for_attempts != b.id:
                        self._wait_in_batch_for_attempts = b.id
                        self._wait_in_sent_attempts = set()
                        self._wait_in_attempt_id = None
                        self._wait_in_attempt_seen_at = 0.0

                    # attempt id: which loading column/position RTK is checking now
                    in_attempt = 0
                    v = getattr(snap, "tarein", None)
                    if v is not None:
                        try:
                            in_attempt = int(v)
                        except Exception:
                            in_attempt = 0

                    # start delay timer on attempt change
                    if self._wait_in_attempt_id != in_attempt:
                        self._wait_in_attempt_id = in_attempt
                        self._wait_in_attempt_seen_at = now

                    # after delay -> read sensors and send ONCE for this attempt
                    if (
                        int(in_attempt) not in self._wait_in_sent_attempts
                        and (now - float(self._wait_in_attempt_seen_at)) >= float(self._wait_sensor_delay_sec)
                    ):
                        cols = sorted({cell_to_col(x, 3) for x in in_ids})
                        ok_any = False

                        for c in cols:
                            try:
                                if self.io.read_loading_column_sensor(int(c)):
                                    ok_any = True
                                    break
                            except Exception as e:
                                print("IN_TARE_SENSOR exception", e)

                        self.rtk.request(
                            "sensor_state",
                            {"SensorName": "stockerintaresensor", "State": bool(ok_any)},
                        )

                        repo.add_event(
                            db,
                            severity=Severity.info.value,
                            source="DAEMON",
                            type_=EventType.RTK_COMMAND_SENT.value,
                            payload={
                                "cmd": "sensor_state",
                                "SensorName": "stockerintaresensor",
                                "batch_id": b.id,
                                "attempt": int(in_attempt),
                                "cols": cols,
                                "State": bool(ok_any),
                            },
                        )

                        self._wait_in_sent_attempts.add(int(in_attempt))
                        self._wait_in_last_send_at = now
                        self._wait_in_last_state = bool(ok_any)      

                # выгрузка: 16 ячеек => 4 колонны
                if a1 == "waitoutstockersensor" and out_ids:
                    # reset per-batch attempt memory
                    if self._wait_out_batch_for_attempts != b.id:
                        self._wait_out_batch_for_attempts = b.id
                        self._wait_out_sent_attempts = set()
                        self._wait_out_attempt_id = None
                        self._wait_out_attempt_seen_at = 0.0

                    # attempt id: RTK tare index (changes when it switches to the next tare)
                    out_attempt_i = 0
                    v = getattr(snap, "tareout", None)
                    if v is not None:
                        try:
                            out_attempt_i = int(v)
                        except Exception:
                            out_attempt_i = 0

                    # start delay timer on attempt change
                    if self._wait_out_attempt_id != out_attempt_i:
                        self._wait_out_attempt_id = out_attempt_i
                        self._wait_out_attempt_seen_at = now

                    # after delay -> read sensors and send ONCE for this attempt
                    if (
                        int(out_attempt_i) not in self._wait_out_sent_attempts
                        and (now - float(self._wait_out_attempt_seen_at)) >= float(self._wait_sensor_delay_sec)
                    ):
                        cols = sorted({cell_to_col(x, 4) for x in out_ids})
                        ok_any = False
                        for c in cols:
                            try:
                                if self.io.read_unloading_column_sensor(int(c)):
                                    ok_any = True
                                    break
                            except Exception as e:
                                print("OUT_TARE_SENSOR exception", e)

                        self.rtk.request(
                            "sensor_state",
                            {"SensorName": "stockerouttaresensor", "State": bool(ok_any)},
                        )
                        repo.add_event(
                            db,
                            severity=Severity.info.value,
                            source="DAEMON",
                            type_=EventType.RTK_COMMAND_SENT.value,
                            payload={
                                "cmd": "sensor_state",
                                "SensorName": "stockerouttaresensor",
                                "batch_id": b.id,
                                "attempt": int(out_attempt_i),
                                "cols": cols,
                                "State": bool(ok_any),
                            },
                        )

                        self._wait_out_sent_attempts.add(int(out_attempt_i))
                        self._wait_out_last_send_at = now
                        self._wait_out_last_state = bool(ok_any)

                                               
        # --- fire scheduled calibration after threshold reject was sent to RTK ---
        if self._calib_due_at is not None and now >= float(self._calib_due_at):
            st3 = repo.ensure_state_row(db)

            if (
                st3.mode == SystemMode.auto_running.value
                and st3.active_batch_id is not None
                and int(st3.active_batch_id) == int(self._calib_due_batch or -1)
                and (not bool(getattr(st3, "calibration_inflight", 0)))
                and str(getattr(st3, "active_operation_id", "") or "") == str(self._calib_due_operation_id or "")                
            ):
                reject_threshold = int(
                    self._calib_due_reject_threshold
                    or self._consecutive_rejects_threshold
                )
                etalon_id = int(self._calib_due_etalon_id)
                self.rtk.request("calibrate", {"etalon_id": etalon_id})
                repo.set_state(
                    db,
                    calibration_inflight=0,
                    calibration_reason="three_consecutive_rejects_im",
                    consecutive_rejects=0,
                    rtk_consecutive_defects=0,
                )
                _set_message(
                    db,
                    f"Критерий брака: достигнут порог подряд идущих "
                    f"браков ({reject_threshold}); запрошена проверка",
                    Severity.warn,
                    message_key=(
                        _batch_operator_message_key(
                            QUALITY_CALIBRATION_MESSAGE_PREFIX,
                            st3.active_batch_id,
                        )
                        or QUALITY_CALIBRATION_MESSAGE_PREFIX
                    ),
                )
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.CALIBRATION_REQUESTED.value,
                    payload={
                        "reason": "three_consecutive_rejects_im",
                        "etalon_id": etalon_id,
                        "batch_id": int(st3.active_batch_id),
                        "operation_id": getattr(st3, "active_operation_id", None),
                        "consecutive_rejects_threshold": reject_threshold,
                    },
                )

                self._calib_due_at = None
                self._calib_due_batch = None
                self._calib_due_operation_id = None                
                self._calib_due_reject_threshold = None

            elif st3.mode == SystemMode.paused_rejectbin.value:
                # Не теряем критерий во время замены полной тары.
                # Он будет отправлен в RESUME_SYSTEM перед rtk_resume.
                pass

            else:
                # Партия/режим уже поменялись — старый due больше невалиден.
                self._calib_due_at = None
                self._calib_due_batch = None
                self._calib_due_operation_id = None                
                self._calib_due_reject_threshold = None


        # --- edge: trash restored ---
        if trash_present:
            repo.clear_operator_message(
                db,
                "trash.missing",
            )

        if self._last_trash_present is None:
            self._last_trash_present = trash_present
        elif self._last_trash_present is False and trash_present is True:
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.TRASHCAN_RESTORED.value,
                payload={
                    "message_key": "trash.missing",
                },
            )

            st_trash = repo.ensure_state_row(db)
            if (
                st_trash.mode
                == SystemMode.paused_trash_missing.value
            ):
                self._set_message_if_changed(
                    db,
                    "Тара брака установлена; нажмите ПРОДОЛЖИТЬ",
                    Severity.info,
                    message_key="trash.recovered",
                )

            self._last_trash_present = True
        else:
            self._last_trash_present = trash_present
    
        # --- reject bin: independent sticky near-full warning ---
        warn_reject_near = False
        if self.reject_bin_capacity > 0:
            thr = self.reject_bin_capacity - int(
                self._rjb_near_full or 0
            )
            if thr < 0:
                thr = 0

            warn_reject_near = (
                reject_count >= thr
                and int(self._rjb_near_full or 0) > 0
                and reject_count < self.reject_bin_capacity
            )

        if warn_reject_near:
            near_full_message = (
                "Тара брака почти заполнена "
                f"({reject_count}/{self.reject_bin_capacity})"
            )
            _set_message(
                db,
                near_full_message,
                Severity.warn,
                message_key="reject_bin.near_full",
            )

            if not self._near_full_emitted:
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.REJECTBIN_NEAR_FULL.value,
                    payload={
                        "message": near_full_message,
                        "message_key": "reject_bin.near_full",
                        "count": reject_count,
                        "capacity": self.reject_bin_capacity,
                        "rjb_near_full": int(
                            self._rjb_near_full or 0
                        ),
                    },
                )
            self._near_full_emitted = True
        else:
            repo.clear_operator_message(
                db,
                "reject_bin.near_full",
            )
            self._near_full_emitted = False

        # --- temperature check ---
        temperature_status = str(
            temperature_state.get("overall_status")
            or "missing"
        )
        temperature_ok = temperature_status in {
            "ok",
            "near_critical",
        }
        temperature_near = (
            temperature_status == "near_critical"
        )

        self._update_temperature_events(
            db,
            temperature_state,
            where="TICK",
            publish_near_message=(
                st.mode
                in {
                    SystemMode.idle.value,
                    SystemMode.manual.value,
                    SystemMode.auto_running.value,
                }
            ),
        )

        # warning during auto (color=5):
        # near reject full OR temperature near critical
        # OR controlled cleanup after operator STOP.
        stop_cleanup_active = (
            str(st.active_batch_phase or "")
            == STOP_CLEANUP_PHASE
        )
        system_warn = (
            st.mode == SystemMode.auto_running.value
            and (
                bool(warn_reject_near)
                or bool(temperature_near)
                or bool(positioner_missing)
                or stop_cleanup_active
            )
        )

        # Синхронизируем аппаратный АвКонтур до веток с ранним return.
        # Это позволяет одновременно показать safety.circuit и air.pressure.
        if postamat_connected and not safety_ok:
            safety_msg = self._format_safety_problem("ПАУЗА[АвКонтур]:")
            self._update_safety_pause_message(
                db,
                safety_msg=safety_msg,
                safety_status_code=safety_status_code,
                safety_status_info=safety_status_info,
            )
        else:
            repo.clear_operator_message(db, "safety.circuit")
            self._last_safety_pause_message = None

        # --- air pressure emergency ---
        # Давление воздуха - авария. Если пропало во время авторазбора,
        # ставим систему в paused_no_air и отправляем RTK pause.
        # Автоматически НЕ возобновляем: после восстановления оператор нажимает ПРОДОЛЖИТЬ.
        st_air = repo.ensure_state_row(db)
        
        if not air_pressure_ok:
            msg = self._format_air_pressure_problem()
        
            if self._last_air_pressure_ok is not False:
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "AIR_PRESSURE_LOST",
                        "source": "RTK data/io/DI09",
                        "air_pressure_ok": False,
                        "mode": st_air.mode,
                        "active_batch_id": st_air.active_batch_id,
                    },
                )
        
            self._last_air_pressure_ok = False
        
            should_pause_auto_for_air = (
                st_air.mode == SystemMode.auto_running.value
                or st_air.mode == SystemMode.paused_no_air.value
            )
        
            if should_pause_auto_for_air:
                self._pause_to(
                    db,
                    SystemMode.paused_no_air.value,
                    msg,
                    EventType.SYSTEM_PAUSED.value,
                    Severity.error.value,
                    message_key="air.pressure",
                )
        
                self._set_message_if_changed(
                    db,
                    msg,
                    Severity.error,
                    message_key="air.pressure",
                )
        
                self._air_pressure_emergency_active = True
        
                self._apply_signals(
                    db,
                    now=now,
                    mode=SystemMode.paused_no_air.value,
                    safety_ok=safety_ok,
                    trash_present=trash_present,
                    reject_count=reject_count,
                    cs_r1=cs1,
                    cs_r2=cs2,
                    system_warn=system_warn,
                    temperature_bad=(not temperature_ok),
                    air_pressure_ok=False,
                    ppod_tripped=bool(self._ppod_tripped_batch_id),
                )
                return
        
            # Воздуха нет, но автоцикл сейчас не был запущен.
            # Не переводим систему в paused_no_air и не создаём сценарий "ПРОДОЛЖИТЬ".
            self._air_pressure_emergency_active = False
        
            self._set_message_if_changed(
                db,
                msg + "; старт невозможен",
                Severity.error,
                message_key="air.pressure",
            )
        
            self._apply_signals(
                db,
                now=now,
                mode=st_air.mode,
                safety_ok=safety_ok,
                trash_present=trash_present,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,
                temperature_bad=(not temperature_ok),
                air_pressure_ok=False,
                ppod_tripped=bool(self._ppod_tripped_batch_id),
            )
            return      
        
        # Давление восстановилось. Аппаратный sticky-ключ снимаем сразу;
        # при реальной no-air паузе отдельно оставляем процессную подсказку
        # о необходимости нажать ПРОДОЛЖИТЬ.
        repo.clear_operator_message(db, "air.pressure")

        # "Нажмите ПРОДОЛЖИТЬ" показываем только после настоящей no-air паузы.
        # Если paused_no_air остался старым/ложным состоянием, очищаем его автоматически.
        st_air = repo.ensure_state_row(db)
        keep_paused_no_air = False
        
        if self._last_air_pressure_ok is False:
            was_no_air_pause = bool(self._air_pressure_emergency_active)
        
            self._last_air_pressure_ok = True
            self._air_pressure_emergency_active = False
        
            if was_no_air_pause and st_air.mode == SystemMode.paused_no_air.value:
                keep_paused_no_air = True

                _set_message(
                    db,
                    "Давление воздуха в норме; нажмите кнопку ПРОДОЛЖИТЬ",
                    Severity.info,
                    message_key="air.recovered",
                )

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "Давление воздуха в норме; нажмите кнопку ПРОДОЛЖИТЬ",
                        "where": "AIR_PRESSURE_RESTORED",
                        "source": "RTK data/io/DI09",
                        "air_pressure_ok": True,
                        "mode": st_air.mode,
                        "was_no_air_pause": True,
                    },
                )

                # после реальной аварии по воздуху оставляем систему в paused_no_air,
                # но сразу показываем оператору восстановление давления и гасим
                # аварийную свето-звуковую индикацию.
                self._apply_signals(
                    db,
                    now=now,
                    mode=SystemMode.paused_no_air.value,
                    safety_ok=safety_ok,
                    trash_present=trash_present,
                    reject_count=reject_count,
                    cs_r1=cs1,
                    cs_r2=cs2,
                    system_warn=system_warn,
                    temperature_bad=(not temperature_ok),
                    air_pressure_ok=True,
                    ppod_tripped=bool(self._ppod_tripped_batch_id),
                )
                return
        
            else:
                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "Давление воздуха в норме",
                        "where": "AIR_PRESSURE_RESTORED",
                        "source": "RTK data/io/DI09",
                        "air_pressure_ok": True,
                        "mode": st_air.mode,
                        "was_no_air_pause": bool(was_no_air_pause),
                    },
                )
        
        elif self._last_air_pressure_ok is None:
            self._last_air_pressure_ok = True
        
        
        # Защита от старого/ложного paused_no_air:
        # если давление сейчас OK, а настоящую no-air паузу в этом tick мы не держим,
        # то система не должна оставаться в paused_no_air и включать аварийную индикацию.
        st_air = repo.ensure_state_row(db)
        
        if (
            air_pressure_ok
            and st_air.mode == SystemMode.paused_no_air.value
            and not keep_paused_no_air
        ):
            repo.clear_operator_message(db, "air.recovered")
            target_mode = (
                self._resume_target_mode
                or (
                    SystemMode.auto_running.value
                    if st_air.active_batch_id is not None
                    else SystemMode.idle.value
                )
            )
        
            _set_transient_message(
                db,
                "Давление воздуха в норме",
                Severity.info,
                message_key="notice.air",
                mode=target_mode,
            )
        
            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "message": "stale paused_no_air cleared",
                    "mode": target_mode,
                    "air_pressure_ok": True,
                    "active_batch_id": st_air.active_batch_id,
                },
            )


        # --- keep paused_safety state in sync with the physical circuit ---
        # safety.circuit существует только при реальной аппаратной аварии.
        # Потеря связи с постаматами отображается отдельным equipment.postamat.
        st = repo.ensure_state_row(db)
        if st.mode == SystemMode.paused_safety.value:
            if not postamat_connected:
                repo.clear_operator_message(db, "safety.circuit")
                self._last_safety_pause_message = None

            elif not safety_ok:
                safety_msg = self._format_safety_problem("ПАУЗА[АвКонтур]:")
                self._update_safety_pause_message(
                    db,
                    safety_msg=safety_msg,
                    safety_status_code=safety_status_code,
                    safety_status_info=safety_status_info,
                )

            else:
                # Физическая авария снята. Сам режим paused_safety сохраняем
                # до подтверждения оператором, но аппаратный sticky-ключ удаляем.
                repo.clear_operator_message(db, "safety.circuit")
                self._last_safety_pause_message = None
                self._set_message_if_changed(
                    db,
                    "Аварийный контур восстановлен; нажмите ПРОДОЛЖИТЬ",
                    Severity.info,
                    message_key="safety.recovered",
                )

            self._apply_signals(
                db,
                now=now,
                mode=SystemMode.paused_safety.value,
                safety_ok=safety_ok,
                trash_present=trash_present,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,
                temperature_bad=(not temperature_ok),
                air_pressure_ok=air_pressure_ok,
                ppod_tripped=bool(self._ppod_tripped_batch_id),
            )
            return

        # --- global pauses (priority) ---
        if not safety_ok:
            repo.clear_operator_message(db, "safety.recovered")
            if postamat_connected:
                safety_msg = self._format_safety_problem("ПАУЗА[АвКонтур]:")
                safety_message_key = "safety.circuit"
            else:
                suffix = f" ({postamat_error})" if postamat_error else ""
                safety_msg = f"Постаматы: потеря связи{suffix}"
                safety_message_key = "equipment.postamat"
                repo.clear_operator_message(db, "safety.circuit")
                self._last_safety_pause_message = None

            self._pause_to(
                db,
                SystemMode.paused_safety.value,
                safety_msg,
                EventType.SAFETY_TRIPPED.value,
                Severity.error.value,
                message_key=safety_message_key,
            )

            if safety_message_key == "safety.circuit":
                self._update_safety_pause_message(
                    db,
                    safety_msg=safety_msg,
                    safety_status_code=safety_status_code,
                    safety_status_info=safety_status_info,
                )

            self._apply_signals(
                db,
                now=now,
                mode=SystemMode.paused_safety.value,
                safety_ok=safety_ok,
                trash_present=trash_present,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,
                temperature_bad=(not temperature_ok),
                air_pressure_ok=air_pressure_ok,
                ppod_tripped=bool(self._ppod_tripped_batch_id),
            )
            return

        repo.clear_operator_message(db, "safety.circuit")
        self._last_safety_pause_message = None


        # --- reject bin full: wait until R2 physically reaches PutToDefect ---
        st = repo.ensure_state_row(db)
        reject_count = int(st.reject_bin_count or 0)

        # Во время stopping_after_stop не запускаем обычную паузу
        # полной тары на промежуточном состоянии R2.
        # Сначала оба робота должны дойти до waitingforstart.
        # После этого ветка STOP_CLEANUP_PHASE ниже сама проверит
        # заполненность тары и при необходимости начнёт её замену.
        stop_cleanup_active = (
            str(st.active_batch_phase or "")
            == STOP_CLEANUP_PHASE
        )

        if (
            not stop_cleanup_active
            and self._request_rejectbin_full_pause_when_safe(
                db,
                reject_count=reject_count,
                a2=a2,
            )
        ):
            st = repo.ensure_state_row(db)

        # --- reject bin unload has priority over trash_present ---
        # Во время замены тара брака физически может отсутствовать.
        # Не даём global trash_missing перетереть сообщения сценария замены.
        st = repo.ensure_state_row(db)
        if st.mode == SystemMode.paused_rejectbin.value:
            # Отсутствие тары внутри штатной процедуры замены не является
            # глобальной аварией trash.missing.
            repo.clear_operator_message(db, "trash.missing")
            idx = 16

            if self._rejectbin_reset_done:
                workflow_message_exists = any(
                    str(item.get("key") or "")
                    == REJECT_BIN_WORKFLOW_MESSAGE_KEY
                    for item in repo.list_operator_messages(db)
                )

                if not workflow_message_exists:
                    self._set_message_if_changed(
                        db,
                        self._rejectbin_unload_message(
                            stage="wait_resume"
                        ),
                        Severity.warn,
                        message_key=(
                            REJECT_BIN_WORKFLOW_MESSAGE_KEY
                        ),
                    )
                self._apply_signals(
                    db,
                    now=now,
                    mode=SystemMode.paused_rejectbin.value,
                    safety_ok=safety_ok,
                    trash_present=True,  # для сигнализации: отсутствие тары во время замены не авария
                    reject_count=reject_count,
                    cs_r1=cs1,
                    cs_r2=cs2,
                    system_warn=system_warn,
                    temperature_bad=(not temperature_ok),
                    air_pressure_ok=air_pressure_ok,
                    ppod_tripped=bool(self._ppod_tripped_batch_id),
                )
                return

            if not self._rejectbin_unload_requested:
                self._begin_rejectbin_unload(
                    db,
                    reason="rejectbin_full",
                    severity=Severity.error,
                )

            if not self._rtk_r2_allows_rejectbin_door(db, a2):
                self._set_message_if_changed(
                    db,
                    self._rejectbin_unload_message(stage="wait_r2"),
                    Severity.warn,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )
                self._apply_signals(
                    db,
                    now=now,
                    mode=SystemMode.paused_rejectbin.value,
                    safety_ok=safety_ok,
                    trash_present=True,
                    reject_count=reject_count,
                    cs_r1=cs1,
                    cs_r2=cs2,
                    system_warn=system_warn,
                    temperature_bad=(not temperature_ok),
                    air_pressure_ok=air_pressure_ok,
                    ppod_tripped=bool(self._ppod_tripped_batch_id),
                )
                return

            if not self._rejectbin_door_open_requested:
                self.io.request_open_loading_cell(
                    int(idx)
                )

                self._rejectbin_door_open_requested = True
                self._rejectbin_door_open_requested_at = float(
                    now
                )
                self._rejectbin_door_seen_open = False
                self._rejectbin_door_closed_since = None
                self._rejectbin_tare_seen_absent = False

                self._rejectbin_reopen_requested_after_missing = (
                    False
                )

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=(
                        EventType
                        .REJECTBIN_DOOR_OPEN_REQUESTED
                        .value
                    ),
                    payload={
                        "side": "rejectbin",
                        "idx": idx,
                        "reason": (
                            self._rejectbin_unload_reason
                            or "rejectbin_unload"
                        ),
                        "r2_action": a2,
                    },
                )

                _set_message(
                    db,
                    self._rejectbin_unload_message(
                        stage="door_opening"
                    ),
                    Severity.warn,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )

                self._apply_signals(
                    db,
                    now=now,
                    mode=SystemMode.paused_rejectbin.value,
                    safety_ok=safety_ok,
                    trash_present=True,
                    reject_count=reject_count,
                    cs_r1=cs1,
                    cs_r2=cs2,
                    system_warn=system_warn,
                    temperature_bad=(not temperature_ok),
                    air_pressure_ok=air_pressure_ok,
                    ppod_tripped=bool(
                        self._ppod_tripped_batch_id
                    ),
                )
                return

            door_closed = (
                self.io.read_loading_door_status(
                    int(idx)
                )
            )

            if door_closed is False:
                self._rejectbin_door_seen_open = True
                self._rejectbin_door_closed_since = None

                self._rejectbin_reopen_requested_after_missing = (
                    False
                )

                trash_now = bool(
                    self.io.read_trashcan_present()
                )

                # Ключевая проверка:
                # старая тара действительно исчезала с датчика.
                if not trash_now:
                    self._rejectbin_tare_seen_absent = True

                if not self._rejectbin_tare_seen_absent:
                    rejectbin_msg = (
                        self._rejectbin_unload_message(
                            stage="remove_old_tare"
                        )
                    )

                elif not trash_now:
                    rejectbin_msg = (
                        self._rejectbin_unload_message(
                            stage="install_new_tare"
                        )
                    )

                else:
                    rejectbin_msg = (
                        self._rejectbin_unload_message(
                            stage="close_door"
                        )
                    )

                self._set_message_if_changed(
                    db,
                    rejectbin_msg,
                    Severity.warn,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )

            elif door_closed is True:
                trash_now = bool(
                    self.io.read_trashcan_present()
                )

                # Дверь пока физически не была замечена открытой.
                if not self._rejectbin_door_seen_open:
                    self._rejectbin_door_closed_since = None

                    open_wait_sec = max(
                        0.0,
                        float(now)
                        - float(
                            self
                            ._rejectbin_door_open_requested_at
                            or 0.0
                        ),
                    )

                    # Даём приводу время выполнить команду.
                    if open_wait_sec < 3.0:
                        self._set_message_if_changed(
                            db,
                            self._rejectbin_unload_message(
                                stage="door_opening"
                            ),
                            Severity.warn,
                            message_key=(
                                REJECT_BIN_WORKFLOW_MESSAGE_KEY
                            ),
                        )

                    else:
                        self._request_rejectbin_door_reopen_once(
                            db,
                            idx=int(idx),
                            reason=(
                                "rejectbin_door_open_"
                                "not_confirmed"
                            ),
                        )

                        self._rejectbin_tare_seen_absent = False

                        self._set_message_if_changed(
                            db,
                            (
                                "Открытие дверцы тары брака "
                                "не подтверждено. Дверца "
                                "открывается повторно; извлеките "
                                "старую тару, установите пустую "
                                "и закройте дверцу."
                            ),
                            Severity.error,
                            message_key=(
                                REJECT_BIN_WORKFLOW_MESSAGE_KEY
                            ),
                        )

                else:
                    if self._rejectbin_door_closed_since is None:
                        self._rejectbin_door_closed_since = float(now)

                    close_confirmed = (
                        float(now)
                        - float(self._rejectbin_door_closed_since)
                        >= float(
                            self._rejectbin_door_close_debounce_sec
                        )
                    )

                    if not close_confirmed:
                        self._set_message_if_changed(
                            db,
                            (
                                "Закройте дверцу тары брака "
                                "до конца. Ожидание устойчивого "
                                "сигнала закрытия."
                            ),
                            Severity.warn,
                            message_key=(
                                REJECT_BIN_WORKFLOW_MESSAGE_KEY
                            ),
                        )

                    # Дверь открывалась, но старая тара ни разу
                    # не исчезала с датчика. Проверяем это только
                    # после устойчивого подтверждения закрытия.
                    elif not self._rejectbin_tare_seen_absent:
                        self._request_rejectbin_door_reopen_once(
                            db,
                            idx=int(idx),
                            reason=(
                                "rejectbin_old_tare_not_removed"
                            ),
                        )

                        self._set_message_if_changed(
                            db,
                            (
                                "Старая тара брака не была "
                                "извлечена. Дверца открывается "
                                "повторно; извлеките старую тару, "
                                "установите пустую и закройте "
                                "дверцу."
                            ),
                            Severity.error,
                            message_key=(
                                REJECT_BIN_WORKFLOW_MESSAGE_KEY
                            ),
                        )

                    # Старая тара была снята, но новая при
                    # устойчиво закрытой двери отсутствует.
                    elif not trash_now:
                        self._request_rejectbin_door_reopen_once(
                            db,
                            idx=int(idx),
                            reason=(
                                "rejectbin_new_tare_missing_"
                                "after_close"
                            ),
                        )

                        self._set_message_if_changed(
                            db,
                            (
                                "Тара брака отсутствует. "
                                "Дверца открывается повторно; "
                                "установите пустую тару и "
                                "закройте дверцу."
                            ),
                            Severity.error,
                            message_key=(
                                REJECT_BIN_WORKFLOW_MESSAGE_KEY
                            ),
                        )

                    else:
                        new_tare_no = (
                            _normalize_rejectbin_tare_no(
                                repo.get_setting(
                                    db,
                                    REJECT_BIN_REPLACEMENT_TARE_SETTING,
                                    0,
                                )
                            )
                        )

                        if new_tare_no is None:
                            # Счётчики и reject_bins пока не изменяем.
                            self._set_message_if_changed(
                                db,
                                (
                                    "Введите номер установленной "
                                    "тары брака "
                                    f"от {REJECT_BIN_TARE_MIN} "
                                    f"до {REJECT_BIN_TARE_MAX}"
                                ),
                                Severity.error,
                                message_key=(
                                    REJECT_BIN_WORKFLOW_MESSAGE_KEY
                                ),
                            )

                        else:
                            self._finish_rejectbin_unload(db)
                            reject_count = 0

            else:
                self._rejectbin_door_closed_since = None
                self._set_message_if_changed(
                    db,
                    (
                        "Не удалось определить состояние "
                        "дверцы тары брака. Проверьте "
                        "дверцу и датчик."
                    ),
                    Severity.error,
                    message_key=REJECT_BIN_WORKFLOW_MESSAGE_KEY,
                )

            self._apply_signals(
                db,
                now=now,
                mode=SystemMode.paused_rejectbin.value,
                safety_ok=safety_ok,
                trash_present=True,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,
                temperature_bad=(not temperature_ok),
                air_pressure_ok=air_pressure_ok,
                ppod_tripped=bool(
                    self._ppod_tripped_batch_id
                ),
            )
            return

    
        if not trash_present:
            repo.clear_operator_message(db, "trash.recovered")
            trash_missing_message = "ПАУЗА: отсутствует тара брака"
            self._pause_to(
                db,
                SystemMode.paused_trash_missing.value,
                trash_missing_message,
                EventType.TRASHCAN_MISSING.value,
                Severity.error.value,
                message_key="trash.missing",
            )
            self._set_message_if_changed(
                db,
                trash_missing_message,
                Severity.error,
                message_key="trash.missing",
            )
            self._apply_signals(
                db,
                now=now,
                mode=SystemMode.paused_trash_missing.value,
                safety_ok=safety_ok,
                trash_present=trash_present,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,        
                temperature_bad=(not temperature_ok),
                air_pressure_ok=air_pressure_ok,
                ppod_tripped=bool(self._ppod_tripped_batch_id),
            )
            return

        if not temperature_ok:
            repo.clear_operator_message(db, "temperature.recovered")
            if temperature_status == "missing":
                temperature_problem_message = self._temperature_message(
                    temperature_state,
                    "missing",
                )
            else:
                temperature_problem_message = self._temperature_message(
                    temperature_state,
                    "out_of_range",
                )

            self._pause_to(
                db,
                SystemMode.paused_temperature.value,
                temperature_problem_message,
                EventType.SYSTEM_PAUSED.value,
                Severity.error.value,
                message_key="temperature.state",
            )

            # Если причина изменилась уже внутри paused_temperature,
            # обновляем текст без повторной команды pause для РТК.
            self._set_message_if_changed(
                db,
                temperature_problem_message,
                Severity.error,
                message_key="temperature.state",
            )

            self._apply_signals(
                db,
                now=now,
                mode=SystemMode.paused_temperature.value,
                safety_ok=safety_ok,
                trash_present=trash_present,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,                
                temperature_bad=(not temperature_ok),
                air_pressure_ok=air_pressure_ok,
                ppod_tripped=bool(self._ppod_tripped_batch_id),
            )
            return

        # Если температура уже нормализовалась, но система всё ещё
        # находится в температурной паузе, явно просим оператора
        # подтвердить продолжение. Режим и RTK автоматически не возобновляем.
        if st.mode == SystemMode.paused_temperature.value:
            if temperature_status == "near_critical":
                temperature_recovered_message = (
                    self._temperature_message(
                        temperature_state,
                        "near_critical",
                    )
                    + "; нажмите ПРОДОЛЖИТЬ"
                )
                temperature_recovered_severity = Severity.warn
            else:
                temperature_recovered_message = (
                    "Температура в норме; нажмите ПРОДОЛЖИТЬ"
                )
                temperature_recovered_severity = Severity.info

            self._set_message_if_changed(
                db,
                temperature_recovered_message,
                temperature_recovered_severity,
                message_key="temperature.recovered",
            )

            self._apply_signals(
                db,
                now=now,
                mode=SystemMode.paused_temperature.value,
                safety_ok=safety_ok,
                trash_present=trash_present,
                reject_count=reject_count,
                cs_r1=cs1,
                cs_r2=cs2,
                system_warn=system_warn,
                temperature_bad=(not temperature_ok),
                air_pressure_ok=air_pressure_ok,
                ppod_tripped=bool(self._ppod_tripped_batch_id),
            )
            return


        if st.active_batch_id is None:
            inflight = repo.get_inflight_auto_batch(db)
            if inflight:
                expected = int((inflight.data or {}).get("product_count") or (inflight.data or {}).get("qty") or 0)

                # восстанавливаем только если логично продолжать авто
                if st.mode == SystemMode.auto_running.value or (st.mode == SystemMode.idle.value and snap.busy):
                    _set_transient_message(
                        db,
                        f"После перезапуска восстановлена выполняемая партия {inflight.id}",
                        Severity.warn,
                        message_key="notice.recovery",
                        mode=SystemMode.auto_running.value,
                        active_batch_id=inflight.id,
                        active_batch_phase="running",
                        active_batch_expected_count=expected,
                    )
                    repo.add_event(
                        db,
                        severity=Severity.warn.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={"active_batch_id": inflight.id, "recovered": True},
                    )


        # positioner.missing синхронизирован выше, до веток с ранним return.


        # --- active batch lifecycle (finish by IM count; RTK as secondary) ---
        st = repo.ensure_state_row(db)
        
        if st.mode == SystemMode.auto_running.value and st.active_batch_id is not None:
            bid = int(st.active_batch_id)
            b = repo.get_batch(db, bid)
            bd = dict((b.data or {}) if b else {})
        
            expected = int(st.active_batch_expected_count or bd.get("product_count") or 0)
            measured = int(bd.get("measured_qty") or 0)
        
            phase = str(
                st.active_batch_phase or ""
            ).strip().lower()

            operation_phase = str(
                getattr(
                    st,
                    "active_operation_phase",
                    "",
                )
                or ""
            ).strip().lower()

            # RTK_START уже принят Supervisor и поставлен во внутреннюю очередь RTK-порта.
            rtk_start_sent = operation_phase in {
                "rtk_start_sent",
                "running",
                "finishing",
            }

            # Фактическая активность РТК: после RTK_START хотя бы один робот вышел из исходного waitingforstart.
            robots_left_waiting_for_start = (
                (
                    bool(a1)
                    and a1 != "waitingforstart"
                )
                or (
                    bool(a2)
                    and a2 != "waitingforstart"
                )
            )

            activity_now = (
                measured > 0
                or (
                    rtk_start_sent
                    and robots_left_waiting_for_start
                )
            )

            # starting -> running
            if phase == "starting":
                if activity_now:
                    repo.set_state(
                        db,
                        active_batch_phase="running",
                        active_operation_phase="running",
                    )

                    repo.add_event(
                        db,
                        severity=Severity.info.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={
                            "message": (
                                "RTK activity confirmed"
                            ),
                            "batch_id": int(bid),
                            "operation_id": getattr(
                                st,
                                "active_operation_id",
                                None,
                            ),
                            "measured": int(measured),
                            "r1_action": a1,
                            "r2_action": a2,
                        },
                    )

            # running -> finishing
            elif phase == "running":
                if expected > 0 and measured >= expected:
                    # Штатный сценарий: ИМ учла ожидаемое количество.
                    _set_transient_message(
                        db,
                        (
                            f"Окончание разбора партии, "
                            "ждем останов РТК"
                        ),
                        Severity.info,
                        message_key="notice.batch",
                        active_batch_phase="finishing",
                        active_operation_phase="finishing",
                    )

                elif (
                    expected > 0
                    and measured < expected
                    and a1 == "waitingforstart"
                    and a2 == "waitingforstart"
                ):
                    # Попасть в phase=running можно только после
                    # подтверждённой физической активности роботов.
                    # Следовательно, это уже возврат в waitingforstart,
                    # а не исходное состояние перед RTK_START.
                    _set_message(
                        db,
                        (
                            f"РТК завершил разбор партии, "
                            f"но количество деталей не совпало: "
                            f"введено {expected}, "
                            f"измерено {measured}. "
                            "Партия будет отклонена"
                        ),
                        Severity.error,
                        message_key=(
                            _batch_operator_message_key(
                                BATCH_RESULT_MESSAGE_PREFIX,
                                bid,
                            )
                            or BATCH_RESULT_MESSAGE_PREFIX
                        ),
                        active_batch_phase="finishing",
                        active_operation_phase="finishing",
                    )

                    repo.add_event(
                        db,
                        severity=Severity.error.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={
                            "message": (
                                "batch count mismatch detected"
                            ),
                            "batch_id": int(bid),
                            "operation_id": getattr(
                                st,
                                "active_operation_id",
                                None,
                            ),
                            "expected": int(expected),
                            "measured": int(measured),
                            "r1_action": a1,
                            "r2_action": a2,
                        },
                    )

            # STOP cleanup -> rejected only after both robots are safe.
            elif phase == STOP_CLEANUP_PHASE:
                a1_stop = str(
                    getattr(snap, "action_r1", "")
                    or ""
                ).strip().lower()
                a2_stop = str(
                    getattr(snap, "action_r2", "")
                    or ""
                ).strip().lower()


                sync_ok, _, sync_err = (
                    self._sync_stop_cleanup_unmeasured(
                        db,
                        batch=b,
                        snap=snap,
                    )
                )

                if not sync_ok:
                    self._set_message_if_changed(
                        db,
                        (
                            f"СТОП партии : извлечение запрещено; "
                            f"не удалось синхронизировать количество брака"
                            f"({sync_err})"
                        ),
                        Severity.error,
                        message_key=(
                            _batch_operator_message_key(
                                STOP_MESSAGE_PREFIX,
                                bid,
                            )
                            or STOP_MESSAGE_PREFIX
                        ),
                    )

                robots_safe = (
                    a1_stop == "waitingforstart"
                    and a2_stop == "waitingforstart"
                )

                if sync_ok and robots_safe:
                    st_stop = repo.ensure_state_row(db)
                    stop_reject_count = int(
                        st_stop.reject_bin_count or 0
                    )

                    # Если остаточные детали заполнили тару брака,
                    # сначала штатно меняем её и только потом
                    # разрешаем извлечение остановленной партии.
                    if (
                        self.reject_bin_capacity > 0
                        and stop_reject_count
                        >= self.reject_bin_capacity
                    ):
                        self._rejectbin_full_emitted = True
                        repo.set_setting(
                            db,
                            REJECT_BIN_REPLACEMENT_TARE_SETTING,
                            0,
                        )
                        repo.set_setting(
                            db,
                            REJECT_BIN_REPLACEMENT_OPERATOR_SETTING,
                            "",
                        )
                        self._begin_rejectbin_unload(
                            db,
                            reason="rejectbin_full",
                            severity=Severity.error,
                        )
                    else:
                        self._finish_stop_cleanup_batch(
                            db,
                            batch=b,
                            now=now,
                        )            

            # finishing -> done (когда оба робота ждут старт)
            elif phase == "finishing":
                a1 = str(getattr(snap, "action_r1", "") or "").strip().lower()
                a2 = str(getattr(snap, "action_r2", "") or "").strip().lower()
            
                if a1 == "waitingforstart" and a2 == "waitingforstart":
                    # batch finished: robots are stopped (both waiting for start)
                    self._queue_sound(now, 2)
                    # prevent immediate auto-start overriding sound=2 with sound=1 in the same tick
                    self._auto_start_block_until = max(
                        float(getattr(self, "_auto_start_block_until", 0.0)),
                        float(self._sound_hold_until),
                    )

                    # Финализируем последнюю открытую тару до статуса партии.
                    bfin = repo.get_batch(db, bid)
                    if bfin:
                        try:
                            db.refresh(bfin)
                        except Exception:
                            pass

                        self._maybe_finalize_open_tare(
                            db,
                            bfin,
                            reason="cycle_finished",
                        )

                    count_mismatch = (
                        expected > 0
                        and measured != expected
                    )

                    if count_mismatch:
                        reject_reason = (
                            "measured_count_mismatch:"
                            f"expected={int(expected)},"
                            f"measured={int(measured)}"
                        )

                        repo.set_batch_status(
                            db,
                            bid,
                            status="rejected",
                            finished_at=utcnow(),
                            reject_reason=reject_reason,
                        )

                        repo.add_event(
                            db,
                            severity=Severity.error.value,
                            source="DAEMON",
                            type_=EventType.BATCH_REJECTED.value,
                            payload={
                                "batch_id": int(bid),
                                "reason": (
                                    "measured_count_mismatch"
                                ),
                                "expected": int(expected),
                                "measured": int(measured),
                                "extraction_ready": True,
                                "auto_cycle_continues": True,
                            },
                        )

                        finish_message = (
                            f"Партия отклонена: "
                            f"введено {expected}, "
                            f"измерено {measured}; "
                            "при наличии следующей партии "
                            "автоцикл продолжится автоматически"
                        )
                        finish_severity = Severity.error

                    else:
                        repo.set_batch_status(
                            db,
                            bid,
                            status="done",
                            finished_at=utcnow(),
                        )

                        repo.add_event(
                            db,
                            severity=Severity.info.value,
                            source="DAEMON",
                            type_=EventType.BATCH_DONE.value,
                            payload={
                                "batch_id": bid,
                                "by": (
                                    "IM_COUNT+"
                                    "RTK_WAITFORSTART"
                                ),
                                "measured_qty": measured,
                                "expected": expected,
                            },
                        )

                        finish_message = (
                            f"Разбор партии завершен"
                        )
                        finish_severity = Severity.info


                    self._reset_tare_tracking(
                        reason="batch_finished",
                        batch_id=bid,
                    )

                    _clear_batch_operator_messages(
                        db,
                        bid,
                        QUALITY_PPOD_MESSAGE_PREFIX,
                        QUALITY_CALIBRATION_MESSAGE_PREFIX,
                        STOP_MESSAGE_PREFIX,
                        IM_WORKFLOW_MESSAGE_PREFIX,
                        BATCH_START_MESSAGE_PREFIX,
                    )

                    finish_state = {
                        "active_batch_id": None,
                        "active_batch_phase": None,
                        "active_batch_expected_count": None,
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

                    if finish_severity == Severity.info:
                        _set_transient_message(
                            db,
                            finish_message,
                            finish_severity,
                            message_key="notice.batch",
                            **finish_state,
                        )
                    else:
                        _set_message(
                            db,
                            finish_message,
                            finish_severity,
                            message_key=(
                                _batch_operator_message_key(
                                    BATCH_RESULT_MESSAGE_PREFIX,
                                    bid,
                                )
                                or BATCH_RESULT_MESSAGE_PREFIX
                            ),
                            **finish_state,
                        )

                    repo.add_event(
                        db,
                        severity=Severity.info.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={
                            "active_batch_id": None,
                            "active_operation_id": None,
                        },
                    )
        
        elif st.mode != SystemMode.auto_running.value and st.active_batch_phase is not None:
            repo.set_state(db, active_batch_phase=None)


        # --- RTK-based reject streak (defectcount) ---
        st = repo.ensure_state_row(db)
        
        if (
            st.mode == SystemMode.auto_running.value
            and st.active_batch_id is not None
            and str(st.active_batch_phase or "")
            != STOP_CLEANUP_PHASE
        ):
            pc = getattr(snap, "pickcount", None)
            dc = getattr(snap, "defectcount", None)
        
            if pc is not None:
                cur_pc = int(pc)
                cur_dc = int(dc or 0)
        
                prev_pc = st.rtk_pickcount_seen
                prev_dc = st.rtk_defectcount_seen
                
                if prev_pc is None:
                    # ИНИЦИАЛИЗАЦИЯ: если это начало партии и все отобранные детали брак,
                    # то подряд брака = pickcount (обычно 1 на первом тике)
                    consec = cur_pc if (cur_pc > 0 and cur_dc == cur_pc) else 0
                    repo.set_state(db,
                        rtk_pickcount_seen=cur_pc,
                        rtk_defectcount_seen=cur_dc,
                        rtk_consecutive_defects=consec,
                    )
                elif cur_pc > int(prev_pc):
                    delta_pc = cur_pc - int(prev_pc)
                    delta_dc = cur_dc - int(prev_dc or 0)
        
                    # если за период не было брака -> streak=0
                    # если delta_dc == delta_pc -> все новые детали брак -> streak += delta_pc
                    # иначе была смесь ok/nok -> streak=0 (точной последовательности нет)
                    if delta_dc <= 0:
                        consec = 0
                    elif delta_dc >= delta_pc:
                        consec = int(st.rtk_consecutive_defects or 0) + delta_pc
                    else:
                        consec = 0
        
                    repo.set_state(db,
                        rtk_pickcount_seen=cur_pc,
                        rtk_defectcount_seen=cur_dc,
                        rtk_consecutive_defects=consec,
                    )
        
                    # критерий -> запрос калибровки (один раз, пока inflight=1)
                    st2 = repo.ensure_state_row(db)
                    im_consec = int(getattr(st2, "consecutive_rejects", 0) or 0)
                    reject_threshold = int(
                        self._consecutive_rejects_threshold
                    )
                    
                    if (
                        (not bool(st2.calibration_inflight))
                        and consec >= reject_threshold
                        and im_consec >= reject_threshold  # синхронизация с результатами ИМ
                        and self._calib_due_at is None
                    ):
                        etalon_id = 0
                        self.rtk.request("calibrate", {"etalon_id": etalon_id})
                        repo.set_state(
                            db,
                            calibration_inflight=0,
                            calibration_reason="three_consecutive_rejects_rtk",
                            consecutive_rejects=0,       # сброс сразу при отправке калибровки
                            rtk_consecutive_defects=0,   # чтобы не висел старый streak после калибровки
                        )

                        _set_message(
                            db,
                            f"Критерий брака: достигнут порог подряд идущих "
                            f"браков ({reject_threshold}); запрошена проверка",
                            Severity.warn,
                            message_key=(
                                _batch_operator_message_key(
                                    QUALITY_CALIBRATION_MESSAGE_PREFIX,
                                    st2.active_batch_id,
                                )
                                or QUALITY_CALIBRATION_MESSAGE_PREFIX
                            ),
                        )

                        repo.add_event(
                            db,
                            severity=Severity.warn.value,
                            source="DAEMON",
                            type_=EventType.CALIBRATION_REQUESTED.value,
                            payload={
                                "reason": "three_consecutive_rejects_rtk",
                                "etalon_id": etalon_id,
                                "consecutive_rejects_threshold": reject_threshold,
                            },
                        )

        # blocked-партии и batch.<id>.doors синхронизированы выше.

        # --- auto: claim next loaded batch and start (IM_CLEAR_DB -> IM_LOAD_PROGRAM -> RTK_START) ---
        st = repo.ensure_state_row(db)
        
        if st.mode == SystemMode.auto_running.value and st.active_batch_id is None:

            if now < float(getattr(self, "_auto_start_block_until", 0.0)):
                # let end-of-batch sound=2 play fully before starting next batch
                pass
            else:
                # Одноразовый операторский запрос привязан к конкретной
                # партии. Проверяем его перед выбором следующей партии:
                # так одна точка одинаково покрывает штатное завершение,
                # PPOD и STOP.
                raw_stop_after_batch_id = repo.get_setting(
                    db,
                    STOP_AFTER_BATCH_SETTING,
                    0,
                )

                try:
                    stop_after_batch_id = int(
                        raw_stop_after_batch_id or 0
                    )
                except (TypeError, ValueError):
                    stop_after_batch_id = 0

                if stop_after_batch_id > 0:
                    stop_after_batch = repo.get_batch(
                        db,
                        stop_after_batch_id,
                    )
                    stop_after_status = str(
                        getattr(
                            stop_after_batch,
                            "status",
                            "",
                        )
                        or ""
                    ).strip().lower()

                    if stop_after_status in {
                        "done",
                        "rejected",
                        "extracted",
                    }:
                        # Сбрасываем latch до вызова STOP_AUTO, чтобы
                        # запрос оставался строго одноразовым даже при
                        # повторном tick.
                        repo.set_setting(
                            db,
                            STOP_AFTER_BATCH_SETTING,
                            0,
                        )
                        self.handle_command(
                            db,
                            CommandType.STOP_AUTO.value,
                            {
                                "reason": "stop_after_batch",
                                "batch_id": stop_after_batch_id,
                            },
                        )
                        return

                # Для обычного автоматического старта следующей партии
                # сохраняем прежнее требование доступного RTK snapshot.
                if not snap.connected:
                    return

                a1 = str(getattr(snap, "action_r1", "") or "").strip().lower()
                a2 = str(getattr(snap, "action_r2", "") or "").strip().lower()
                rtk_ready = (a1 == "waitingforstart" and a2 == "waitingforstart")
        
                if not rtk_ready:
                    print("RTK not ready")
                    return

                # Не перескакиваем через более раннюю blocked-партию.
                # Пока её двери открыты, сохраняем auto_running и ждём закрытия.
                next_pending_batch = (
                    db.execute(
                        select(BatchRow)
                        .where(
                            BatchRow.status.in_(
                                ["blocked", "loaded"]
                            )
                        )
                        .order_by(BatchRow.id.asc())
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )

                if (
                    next_pending_batch is not None
                    and str(
                        next_pending_batch.status
                    ) == "blocked"
                ):
                    (
                        loading_cell,
                        unloading_cell,
                    ) = _batch_door_cells(
                        next_pending_batch
                    )

                    if (
                        loading_cell is None
                        or unloading_cell is None
                    ):
                        self._set_message_if_changed(
                            db,
                            (
                                f"Партия "
                                f"{int(next_pending_batch.id)} "
                                "не запущена: не определены "
                                "ячейки загрузки или выгрузки"
                            ),
                            Severity.error,
                            message_key=_batch_doors_message_key(
                                int(next_pending_batch.id)
                            ),
                        )
                        return

                    ld_closed = (
                        self.io
                        .read_loading_door_status(
                            int(loading_cell)
                        )
                    )
                    ud_closed = (
                        self.io
                        .read_unloading_door_status(
                            int(unloading_cell)
                        )
                    )

                    (
                        loading_seen_open,
                        unloading_seen_open,
                    ) = _batch_door_seen_flags(
                        next_pending_batch
                    )

                    # Сразу после создания двери ещё могут
                    # физически не успеть открыться.
                    if not (
                        loading_seen_open
                        and unloading_seen_open
                    ):
                        self._set_message_if_changed(
                            db,
                            _batch_doors_open_wait_message(
                                batch_id=int(
                                    next_pending_batch.id
                                ),
                                loading_cell=int(
                                    loading_cell
                                ),
                                unloading_cell=int(
                                    unloading_cell
                                ),
                                loading_seen_open=(
                                    loading_seen_open
                                ),
                                unloading_seen_open=(
                                    unloading_seen_open
                                ),
                            ),
                            Severity.warn,
                            message_key=_batch_doors_message_key(
                                int(next_pending_batch.id)
                            ),
                        )
                        return

                    # Двери уже открывались, теперь ожидаем
                    # их закрытия оператором.
                    if not (
                        ld_closed is True
                        and ud_closed is True
                    ):
                        self._set_message_if_changed(
                            db,
                            _batch_doors_wait_message(
                                batch_id=int(
                                    next_pending_batch.id
                                ),
                                loading_cell=int(
                                    loading_cell
                                ),
                                unloading_cell=int(
                                    unloading_cell
                                ),
                                loading_closed=ld_closed,
                                unloading_closed=ud_closed,
                            ),
                            Severity.warn,
                            message_key=_batch_doors_message_key(
                                int(next_pending_batch.id)
                            ),
                        )
                        return

                    # Безопасный fallback:
                    # признаки открытия сохранены,обе двери уже закрыты, но общий gate
                    # ещё не успел перевести запись в loaded.
                    repo.set_batch_status(
                        db,
                        next_pending_batch.id,
                        status="loaded",
                        loaded_at=utcnow(),
                    )

                b = repo.claim_next_loaded_batch(db)

                if not b:
                    # fallback, если claim почему-то ничего не вернул
                    b = (
                        db.execute(
                            select(BatchRow).where(BatchRow.status == "loaded").order_by(BatchRow.id.asc()).limit(1)
                        )
                        .scalars()
                        .first()
                    )
                if not b:
                    # no more batches -> exit auto to idle
                    self.handle_command(db, CommandType.STOP_AUTO.value, {"reason": "queue_empty"})
                    return

                # Следующая партия уже выбрана для фактического старта.
                # С этого момента итоговые сообщения предыдущих партий
                # должны исчезнуть, даже если те ещё не извлечены.
                _clear_completed_batch_operator_messages(
                    db,
                    keep_batch_id=int(b.id),
                )
            
                data = b.data or {}
                loc = b.location or {}
            
                raw_code = data.get("product_code") or data.get("product_name") or f"batch_{b.id}"
                
                pn_base, pn_spec_from_code = parse_product_name_and_spec(str(raw_code))
                product_spec = int(data.get("product_spec") or pn_spec_from_code or 0)

                product_code_full = str(raw_code).strip() or pn_base
                if product_spec and "-" not in str(raw_code):
                    product_code_full = f"{pn_base}-{product_spec:02d}"

                # В RTK уходит полная пара pn_base + product_spec,
                # а в IM при исполнении грузим базовую программу.
                im_code = pn_base if product_spec != 0 else product_code_full

                product_count = int(data.get("product_count") or data.get("qty") or 0)

                try:
                    raw_layout = (
                        data["layout"]
                        if "layout" in data
                        else repo.get_setting(
                            db,
                            "layout",
                            DEFAULT_BATCH_LAYOUT,
                        )
                    )
                    layout = int(raw_layout)
                except (TypeError, ValueError):
                    layout = DEFAULT_BATCH_LAYOUT

                if layout not in (0, 1, 2, 3):
                    layout = DEFAULT_BATCH_LAYOUT

                if "use_alternate_wave" in data:
                    use_alternate_wave = (
                        data.get(
                            "use_alternate_wave"
                        ) is True
                    )
                else:
                    use_alternate_wave = (
                        self._setting_bool(
                            db,
                            "use_alternate_wave",
                            DEFAULT_USE_ALTERNATE_WAVE,
                        )
                    )

                try:
                    product_rule = resolve_product_rule(
                        product_code=product_code_full,
                        layout=layout,
                        use_alternate_wave=use_alternate_wave,
                        product_count=product_count,
                    )
                except ProductRuleError as exc:
                    error_message = str(exc)
                    repo.set_batch_status(
                        db,
                        b.id,
                        status="rejected",
                        reject_reason="invalid_product_rule",
                    )
                    self._set_message_if_changed(
                        db,
                        f"Партия не запущена: {error_message}",
                        Severity.error,
                        message_key=(
                            _batch_operator_message_key(
                                BATCH_START_MESSAGE_PREFIX,
                                b.id,
                            )
                            or BATCH_START_MESSAGE_PREFIX
                        ),
                    )
                    repo.add_event(
                        db,
                        severity=Severity.error.value,
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={
                            "where": "AUTO_START_PRODUCT_RULE",
                            "batch_id": int(b.id),
                            "product_code": product_code_full,
                            "layout": int(layout),
                            "use_alternate_wave": bool(
                                use_alternate_wave
                            ),
                            "product_count": int(product_count),
                            "err": error_message,
                        },
                    )
                    return

                product_code_full = product_rule.product_code
                pn_base = product_rule.product_name
                product_spec = int(product_rule.product_spec)
                im_code = (
                    pn_base
                    if product_spec != 0
                    else product_code_full
                )

                in_ids = _batch_side_cell_ids(
                    b,
                    side="loading",
                )
                out_ids = _batch_side_cell_ids(
                    b,
                    side="unloading",
                )

                # Перед стартом проверяем двери каждой стороны отдельно.
                (
                    loading_door_idx,
                    unloading_door_idx,
                ) = _batch_door_cells(b)

                try:
                    loading_door_idx = (
                        int(loading_door_idx)
                        if loading_door_idx is not None
                        else None
                    )
                except (TypeError, ValueError):
                    loading_door_idx = None

                try:
                    unloading_door_idx = (
                        int(unloading_door_idx)
                        if unloading_door_idx is not None
                        else None
                    )
                except (TypeError, ValueError):
                    unloading_door_idx = None

                if loading_door_idx is None or unloading_door_idx is None:
                    repo.set_batch_status(db, b.id, status="blocked")
                    self._set_message_if_changed(
                        db,
                        (
                            f"Партия не запущена: "
                            "не определены ячейки загрузки "
                            "или выгрузки"
                        ),
                        Severity.error,
                        message_key=_batch_doors_message_key(
                            int(b.id)
                        ),
                    )                    
                    repo.add_event(
                        db,
                        severity=Severity.error.value,
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={
                            "where": "AUTO_START_BATCH",
                            "err": "missing_cell_ids",
                            "batch_id": b.id,
                            "in_tare_ids": in_ids,
                            "out_tare_ids": out_ids,
                        },
                    )
                    return

                ld_closed = self.io.read_loading_door_status(
                    loading_door_idx
                )
                ud_closed = self.io.read_unloading_door_status(
                    unloading_door_idx
                )

                if not (
                    ld_closed is True
                    and ud_closed is True
                ):
                    repo.set_batch_status(
                        db,
                        b.id,
                        status="blocked",
                    )

                    self._set_message_if_changed(
                        db,
                        _batch_doors_wait_message(
                            batch_id=int(b.id),
                            loading_cell=int(
                                loading_door_idx
                            ),
                            unloading_cell=int(
                                unloading_door_idx
                            ),
                            loading_closed=ld_closed,
                            unloading_closed=ud_closed,
                        ),
                        Severity.warn,
                        message_key=_batch_doors_message_key(
                            int(b.id)
                        ),
                    )

                    repo.add_event(
                        db,
                        severity=Severity.warn.value,
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={
                            "where": "AUTO_START_BATCH",
                            "err": "doors_open",
                            "batch_id": b.id,
                            "loading_cell_no": loading_door_idx,
                            "unloading_cell_no": unloading_door_idx,
                            "loading_door_closed": bool(ld_closed),
                            "unloading_door_closed": bool(ud_closed),
                        },
                    )
                    return
            
                if product_count <= 0 or not in_ids or not out_ids:
                    repo.add_event(
                        db,
                        severity=Severity.error.value,
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={"where": "AUTO_START_BATCH", "err": "missing fields for RTK_START", "batch_id": b.id},
                    )
                    return
                
                # Перед стартом новой партии снимаем PPOD-индикацию прошлой партии.
                # Не сбрасываем _last_sound_cmd/_last_color_cmd: _queue_sound(now, 1)
                # сам выставит одноразовый сигнал старта партии.
                if (
                    self._ppod_tripped_batch_id is not None
                    and int(self._ppod_tripped_batch_id) != int(b.id)
                ):
                    self._clear_ppod_latch(db, reason="next_batch_start")

                # Перед стартом партии давление уже проверено выше в tick().
                # Сбрасываем старый no-air edge, чтобы старт партии не выглядел как восстановление воздуха.
                self._last_air_pressure_ok = True
                self._air_pressure_emergency_active = False
                self._air_pressure_lost_since = None                

                # фиксируем активную партию и создаём уникальный контекст операции сразу
                operation_id = repo.begin_active_operation(
                    db,
                    batch_id=int(b.id),
                    expected_count=int(product_count),
                    phase="starting",
                )

                repo.clear_operator_message(
                    db,
                    _batch_doors_message_key(int(b.id)),
                )
                repo.set_batch_status(db, b.id, status="auto_processing", loaded_at=utcnow())
                repo.clear_operator_message(
                    db,
                    (
                        _batch_operator_message_key(
                            BATCH_START_MESSAGE_PREFIX,
                            b.id,
                        )
                        or BATCH_START_MESSAGE_PREFIX
                    ),
                )

                _set_transient_message(
                    db,
                    f"АВТОЦИКЛ: партия запущена",
                    Severity.info,
                    message_key="notice.batch",
                    active_batch_id=b.id,
                    active_batch_phase="starting",
                    active_batch_expected_count=int(product_count),
                    active_operation_id=operation_id,
                    active_operation_batch_id=int(b.id),
                    active_operation_phase="starting",
                    active_operation_started_at=utcnow(),
                    last_meas_ok=None,
                    last_meas_not_ok=None,
                    last_meas_summary=None,
                )
                self._queue_sound(now, 1)

                repo.add_event(
                    db,
                    severity=Severity.info.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "active_batch_id": b.id,
                        "active_operation_id": operation_id,
                    },
                )

                # 1) очистка журнала измерений ИМ.
                # Выполняется при старте каждой отдельной партии,
                # в том числе при автоматическом переходе к следующей партии.
                _enqueue_cmd(
                    db,
                    CommandType.IM_CLEAR_DB.value,
                    {
                        "batch_id": int(b.id),
                        "operation_id": operation_id,
                        "phase": "batch_start",
                    },
                )
            
                # 2) загрузка измерительной программы в ИМ
                _enqueue_cmd(
                    db,
                    CommandType.IM_LOAD_PROGRAM.value,
                    {
                        "product_code": str(im_code),
                        "batch_id": int(b.id),
                        "operation_id": operation_id,                        
                        "phase": "batch_start",
                    },
                )
            
                # 3) старт цикла на РТК
                _enqueue_cmd(
                    db,
                    CommandType.RTK_START.value,
                    {
                        "ProductName": str(product_code_full),
                        "ProductSpec": int(product_spec),
                        "ProductCount": int(product_count),
                        "InTareIDs": [int(x) for x in in_ids],
                        "OutTareIDs": [int(x) for x in out_ids],
                        "Layout": int(layout),
                        "GlobalMaxTareCount": int(
                            product_rule.global_max_tare_count
                        ),
                        "CurrentMaxTareCount": int(
                            product_rule.current_max_tare_count
                        ),
                        "UseAlternateWave": bool(use_alternate_wave),
                        "batch_id": int(b.id),
                        "operation_id": operation_id,                        
                    },
                )
       
   
        # --- auto tick ---
        st = repo.ensure_state_row(db)
        if st.mode == SystemMode.auto_running.value and (now - self._last_tick) >= self._tick_period:
            self._last_tick = now
    
        # --- signals based on current mode ---
        st = repo.ensure_state_row(db)
        self._apply_signals(
            db,
            now=now,
            mode=st.mode,
            safety_ok=safety_ok,
            trash_present=trash_present,
            reject_count=reject_count,
            cs_r1=cs1,
            cs_r2=cs2,
            system_warn=system_warn,
            temperature_bad=(not temperature_ok),
            air_pressure_ok=air_pressure_ok,
            ppod_tripped=bool(self._ppod_tripped_batch_id),
        )
