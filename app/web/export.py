from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.common.enums import EventType, Severity
from app.infra import repo
from app.infra.db import SessionLocal
from app.infra.models import BatchRow, EventRow, UserRow
from app.web.auth import require_admin


router = APIRouter(prefix="/admin/export", tags=["admin-export"])

EXPORT_DIR_PREFIX = "Postamats_export_"
EXPORT_TEMP_PREFIX = ".Postamats_export_tmp_"
CSV_DELIMITER = ";"


class ExportCreate(BaseModel):
    drive_id: str = Field(..., min_length=8, max_length=128)


class ExportUnmount(BaseModel):
    drive_id: str = Field(..., min_length=8, max_length=128)


def _get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    return str(value)


def _dt_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        # Project timestamps are written through utcnow(). SQLite DateTime may
        # return them without tzinfo, so treat naive DB values as UTC and show
        # the human export in the Ubuntu host's configured local timezone.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        parsed = parsed.astimezone()
        return parsed.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return _as_text(value)


def _json_text(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
    except Exception:
        return _as_text(value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _human_size(value: int | None) -> str:
    size = float(value or 0)
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "Б":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{int(value or 0)} Б"


def _drive_id(*, device: str, mount_path: str) -> str:
    raw = f"{device}\0{mount_path}".encode("utf-8", errors="replace")
    return hashlib.sha256(raw).hexdigest()[:24]


def _iter_lsblk_nodes(nodes: list[dict], *, inherited_usb: bool = False, inherited_removable: bool = False):
    for node in nodes or []:
        transport = str(node.get("tran") or "").strip().lower()
        removable = bool(node.get("rm")) or bool(node.get("hotplug")) or inherited_removable
        usb = transport == "usb" or inherited_usb
        yield node, usb, removable
        children = node.get("children") or []
        if children:
            yield from _iter_lsblk_nodes(
                children,
                inherited_usb=usb,
                inherited_removable=removable,
            )


def _mountpoints_from_node(node: dict) -> list[str]:
    raw = node.get("mountpoints")
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    single = node.get("mountpoint")
    return [str(single)] if single else []


def _discover_via_lsblk() -> list[dict]:
    try:
        proc = subprocess.run(
            [
                "lsblk",
                "--json",
                "--bytes",
                "-o",
                "NAME,KNAME,PATH,TYPE,RM,HOTPLUG,TRAN,MOUNTPOINTS,LABEL,SIZE,FSTYPE,RO,PKNAME",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=4,
            check=True,
        )
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        return []

    found: list[dict] = []
    for node, is_usb, is_removable in _iter_lsblk_nodes(payload.get("blockdevices") or []):
        if not (is_usb or is_removable):
            continue
        if str(node.get("type") or "").lower() in {"loop", "rom"}:
            continue

        for mount in _mountpoints_from_node(node):
            try:
                mount_path = str(Path(mount).resolve(strict=True))
            except Exception:
                continue
            if mount_path == "/" or not os.path.ismount(mount_path):
                continue

            device = str(node.get("path") or "").strip()
            if not device:
                kname = str(node.get("kname") or node.get("name") or "").strip()
                device = f"/dev/{kname}" if kname else "unknown"

            mount_read_only = False
            try:
                vfs = os.statvfs(mount_path)
                free_bytes = int(vfs.f_bavail) * int(vfs.f_frsize)
                total_bytes = int(vfs.f_blocks) * int(vfs.f_frsize)
                mount_read_only = _filesystem_is_read_only(vfs)
            except Exception:
                free_bytes = 0
                total_bytes = _safe_int(node.get("size"), 0)

            device_read_only = bool(node.get("ro"))
            has_write_access = os.access(mount_path, os.W_OK | os.X_OK)
            writable = (
                not device_read_only
                and not mount_read_only
                and has_write_access
            )
            read_only_reason = None
            if device_read_only:
                read_only_reason = "device"
            elif mount_read_only:
                read_only_reason = "filesystem"
            elif not has_write_access:
                read_only_reason = "permissions"

            label = str(node.get("label") or "").strip()
            found.append(
                {
                    "id": _drive_id(device=device, mount_path=mount_path),
                    "device": device,
                    "label": label,
                    "mount_path": mount_path,
                    "filesystem": str(node.get("fstype") or "").strip(),
                    "writable": bool(writable),
                    "read_only_reason": read_only_reason,
                    "can_unmount": device.startswith("/dev/"),
                    "free_bytes": free_bytes,
                    "total_bytes": total_bytes,
                    "free_human": _human_size(free_bytes),
                    "total_human": _human_size(total_bytes),
                }
            )

    unique: dict[tuple[str, str], dict] = {}
    for drive in found:
        unique[(drive["device"], drive["mount_path"])] = drive
    return sorted(
        unique.values(),
        key=lambda x: (str(x.get("label") or "").casefold(), x["mount_path"]),
    )


def _discover_via_mount_roots() -> list[dict]:
    """Fallback for unusual lsblk output; never treats arbitrary directories as USB."""
    roots = [Path("/media"), Path("/run/media")]
    found: list[dict] = []
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        try:
            candidates = list(root.glob("*")) + list(root.glob("*/*"))
        except Exception:
            continue
        for candidate in candidates:
            try:
                resolved = candidate.resolve(strict=True)
            except Exception:
                continue
            mount_path = str(resolved)
            if not resolved.is_dir() or not os.path.ismount(mount_path):
                continue
            mount_read_only = False
            try:
                vfs = os.statvfs(mount_path)
                free_bytes = int(vfs.f_bavail) * int(vfs.f_frsize)
                total_bytes = int(vfs.f_blocks) * int(vfs.f_frsize)
                mount_read_only = _filesystem_is_read_only(vfs)
            except Exception:
                free_bytes = 0
                total_bytes = 0
            has_write_access = os.access(mount_path, os.W_OK | os.X_OK)
            writable = not mount_read_only and has_write_access
            read_only_reason = None
            if mount_read_only:
                read_only_reason = "filesystem"
            elif not has_write_access:
                read_only_reason = "permissions"
            device = f"mount:{mount_path}"
            found.append(
                {
                    "id": _drive_id(device=device, mount_path=mount_path),
                    "device": device,
                    "label": resolved.name,
                    "mount_path": mount_path,
                    "filesystem": "",
                    "writable": bool(writable),
                    "read_only_reason": read_only_reason,
                    "can_unmount": False,
                    "free_bytes": free_bytes,
                    "total_bytes": total_bytes,
                    "free_human": _human_size(free_bytes),
                    "total_human": _human_size(total_bytes),
                }
            )
    return sorted(found, key=lambda x: x["mount_path"])


def discover_usb_drives() -> list[dict]:
    drives = _discover_via_lsblk()
    if drives:
        return drives
    return _discover_via_mount_roots()


def _resolve_drive(drive_id: str) -> dict:
    for drive in discover_usb_drives():
        if drive["id"] == str(drive_id):
            return drive
    raise HTTPException(
        status_code=409,
        detail=(
            "Выбранный USB-носитель больше не доступен. "
            "Обновите список носителей и повторите экспорт."
        ),
    )


def _batch_status_text(status: str) -> str:
    return {
        "loaded": "Загружена",
        "auto_processing": "В обработке",
        "done": "Разобрана",
        "rejected": "Отклонена",
        "extracted": "Извлечена",
        "blocked": "Заблокирована",
    }.get(str(status), str(status))


def _list_text(values: Any) -> str:
    if not isinstance(values, list):
        return ""
    result: list[str] = []
    for value in values:
        if value in (None, ""):
            continue
        try:
            result.append(str(int(value)))
        except (TypeError, ValueError):
            result.append(str(value))
    return ", ".join(result)


def _out_tares_text(data: dict) -> str:
    values = data.get("extracted_out_tares") or data.get("out_tares") or []
    if not isinstance(values, list):
        return ""
    rows: list[str] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        cell = _safe_int(item.get("cell_no"), 0)
        tare = _safe_int(item.get("tare_no"), 0)
        qty = _safe_int(item.get("qty"), 0)
        if cell > 0 and tare > 0:
            rows.append(f"ячейка {cell}, тара {tare}, кол-во {qty}")
    return " | ".join(rows)


BATCH_COLUMNS = [
    "ID партии",
    "Статус",
    "Маршрутный паспорт",
    "Дата маршрутного паспорта",
    "Наименование детали",
    "Обозначение детали",
    "Исполнение",
    "Сплав",
    "Заготовка",
    "Номер сертификата",
    "Номер партии прутка",
    "Масса",
    "ТУ на слиток",
    "ТУ на заготовку",
    "Номер изменения чертежа",
    "Содержание железа",
    "Доп. параметр 1",
    "Доп. параметр 2",
    "Доп. параметр 3",
    "Доп. параметр 4",
    "Комментарий",
    "Количество в партии",
    "Измерено всего",
    "Годных",
    "Брак",
    "Причина отклонения",
    "Создана",
    "Загружена",
    "Завершена",
    "Извлечена",
    "Загрузочные ячейки",
    "Выгрузочные ячейки",
    "Раскладка годных по тарам",
]


def _batch_record(batch: BatchRow) -> list[str]:
    data = dict(batch.data or {})
    location = dict(batch.location or {})
    in_ids = (
        data.get("extracted_from_in_tare_ids")
        or data.get("in_tare_ids")
        or location.get("in_tare_ids")
        or ([] if location.get("cell_no") is None else [location.get("cell_no")])
    )
    out_ids = (
        data.get("extracted_from_out_tare_ids")
        or data.get("out_tare_ids")
        or location.get("out_tare_ids")
        or []
    )
    good = _safe_int(getattr(batch, "measured_good", None), _safe_int(data.get("ok_qty"), 0))
    bad = _safe_int(getattr(batch, "measured_bad", None), _safe_int(data.get("nok_qty"), 0))
    measured = _safe_int(data.get("measured_qty"), good + bad)

    return [
        _as_text(batch.id),
        _batch_status_text(batch.status),
        _as_text(data.get("passport_number")),
        _as_text(data.get("passport_date")),
        _as_text(data.get("product_name")),
        _as_text(data.get("product_code")),
        _as_text(data.get("product_spec")),
        _as_text(data.get("blank_alloy")),
        _as_text(data.get("blank_name")),
        _as_text(data.get("cert_number")),
        _as_text(data.get("rod_batch_number")),
        _as_text(data.get("items_mass")),
        _as_text(data.get("prod_tsi")),
        _as_text(data.get("prod_tsb")),
        _as_text(data.get("draw_rev")),
        _as_text(data.get("prod_fe")),
        _as_text(data.get("prod_opt1")),
        _as_text(data.get("prod_opt2")),
        _as_text(data.get("prod_opt3")),
        _as_text(data.get("prod_opt4")),
        _as_text(data.get("prod_comment")),
        _as_text(data.get("product_count")),
        _as_text(measured),
        _as_text(good),
        _as_text(bad),
        _as_text(batch.reject_reason),
        _dt_text(batch.created_at),
        _dt_text(batch.loaded_at),
        _dt_text(batch.finished_at),
        _dt_text(batch.extracted_at),
        _list_text(in_ids),
        _list_text(out_ids),
        _out_tares_text(data),
    ]


def _event_batch(payload: dict) -> str:
    for key in ("batch_id", "active_batch_id"):
        value = payload.get(key)
        if value not in (None, ""):
            return _as_text(value)
    target = payload.get("target")
    if isinstance(target, dict):
        for key in ("batch_id", "id"):
            value = target.get(key)
            if value not in (None, ""):
                return _as_text(value)
    return ""


def _event_user(payload: dict) -> str:
    for key in ("display_name", "created_by", "by", "user"):
        value = payload.get(key)
        if value not in (None, ""):
            return _as_text(value)
    return ""


def _event_description(event_type: str, payload: dict) -> str:
    descriptions = {
        EventType.USER_LOGIN.value: "Пользователь вошел в систему",
        EventType.USER_LOGOUT.value: "Пользователь вышел из системы",
        EventType.OPERATOR_CREATED.value: "Администратор создал оператора",
        EventType.OPERATOR_DISABLED.value: "Администратор деактивировал оператора",
        EventType.SETTINGS_UPDATED.value: "Администратор изменил настройки",
        EventType.COMMAND_ACCEPTED.value: "Команда принята daemon",
        EventType.COMMAND_DONE.value: "Команда выполнена",
        EventType.COMMAND_FAILED.value: "Выполнение команды завершилось ошибкой",
        EventType.BATCH_CREATED.value: "Создана партия",
        EventType.BATCH_DONE.value: "Разбор партии завершен",
        EventType.BATCH_REJECTED.value: "Партия отклонена",
        EventType.BATCH_EXTRACTED.value: "Партия извлечена",
        EventType.SYSTEM_PAUSED.value: "Система поставлена на паузу",
        EventType.SYSTEM_RESUMED.value: "Работа системы продолжена",
        EventType.SAFETY_TRIPPED.value: "Нарушен аварийный контур",
        EventType.REJECTBIN_FULL.value: "Тара брака заполнена",
        EventType.REJECTBIN_REPLACED.value: "Тара брака заменена",
        EventType.REJECTBIN_NEAR_FULL.value: "Тара брака близка к заполнению",
        EventType.TEMPERATURE_OUT_OF_RANGE.value: "Температура вышла за допустимый диапазон",
        EventType.TEMPERATURE_NEAR_CRITICAL.value: "Температура приблизилась к критическому значению",
        EventType.TEMPERATURE_UNAVAILABLE.value: "Данные температуры недоступны",
        EventType.RTK_COMMAND_SENT.value: "Команда отправлена РТК",
        EventType.MEASUREMENT_RECORDED.value: "Результат измерения сохранен",
        EventType.CALIBRATION_RESULT_RECORDED.value: "Результат калибровки сохранен",
        EventType.DOCS_PRINTED.value: "Документы партии напечатаны",
        EventType.EXPORT_CREATED.value: "Экспорт данных на USB успешно создан",
        EventType.EXPORT_FAILED.value: "Ошибка экспорта данных на USB",
        EventType.USB_UNMOUNTED.value: "USB-носитель безопасно размонтирован",
        EventType.USB_UNMOUNT_FAILED.value: "Ошибка безопасного размонтирования USB-носителя",
    }
    base = descriptions.get(str(event_type), str(event_type))

    if event_type in {EventType.COMMAND_ACCEPTED.value, EventType.COMMAND_DONE.value, EventType.COMMAND_FAILED.value}:
        command = payload.get("type") or payload.get("command") or payload.get("command_type")
        if command:
            base += f": {command}"
        error = payload.get("error")
        if error:
            base += f"; ошибка: {error}"
    elif event_type == EventType.SETTINGS_UPDATED.value:
        details = payload.get("details")
        changes = details.get("changes") if isinstance(details, dict) else None
        if isinstance(changes, dict) and changes:
            base += ": " + ", ".join(str(k) for k in changes.keys())
    elif event_type == EventType.RTK_COMMAND_SENT.value:
        command = payload.get("command") or payload.get("cmd") or payload.get("type")
        if command:
            base += f": {command}"
    elif event_type in {EventType.OPERATOR_CREATED.value, EventType.OPERATOR_DISABLED.value}:
        target = payload.get("target")
        if isinstance(target, dict):
            name = target.get("display_name") or target.get("full_name")
            if name:
                base += f": {name}"

    return base


EVENT_COLUMNS = [
    "Дата и время",
    "Уровень",
    "Подсистема",
    "Партия",
    "Пользователь",
    "Событие",
    "Описание",
    "Технические данные",
]


def _event_record(event: EventRow) -> list[str]:
    payload = dict(event.payload or {})
    return [
        _dt_text(event.ts),
        _as_text(event.severity),
        _as_text(event.source),
        _event_batch(payload),
        _event_user(payload),
        _as_text(event.type),
        _event_description(str(event.type), payload),
        _json_text(payload),
    ]


def _write_csv(path: Path, header: list[str], rows: Iterable[list[str]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(
            fh,
            delimiter=CSV_DELIMITER,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\r\n",
        )
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
            count += 1
        fh.flush()
        os.fsync(fh.fileno())
    return count


def _build_local_export(db: Session, directory: Path) -> dict:
    # finished_at is the stable marker that a batch has actually completed
    # production processing. Later extraction changes status to extracted but
    # preserves finished_at, so these batches remain exportable.
    batches = (
        db.execute(
            select(BatchRow)
            .where(BatchRow.finished_at.is_not(None))
            .order_by(BatchRow.id.asc())
        )
        .scalars()
        .all()
    )
    events = (
        db.execute(select(EventRow).order_by(EventRow.id.asc()))
        .scalars()
        .all()
    )

    batches_path = directory / "batches.csv"
    events_path = directory / "events.csv"
    batches_count = _write_csv(
        batches_path,
        BATCH_COLUMNS,
        (_batch_record(row) for row in batches),
    )
    events_count = _write_csv(
        events_path,
        EVENT_COLUMNS,
        (_event_record(row) for row in events),
    )
    return {
        "batches_count": batches_count,
        "events_count": events_count,
        "bytes": batches_path.stat().st_size + events_path.stat().st_size,
    }


def _safe_cleanup(path: Path) -> None:
    try:
        if path.exists():
            shutil.rmtree(path)
    except Exception:
        pass


def _filesystem_is_read_only(vfs: os.statvfs_result) -> bool:
    return bool(getattr(os, "ST_RDONLY", 1) & int(vfs.f_flag))


def _sync_usb_filesystem(mount_path: Path) -> None:
    """Flush all pending data and metadata for the selected USB filesystem.

    Per-file fsync is not enough for the final directory rename on every
    removable filesystem. Ubuntu provides GNU coreutils `sync -f`, which
    issues syncfs(2) for the filesystem containing the supplied path.
    An export is not reported as successful until this step succeeds.
    """
    try:
        proc = subprocess.run(
            ["sync", "-f", str(mount_path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
    except FileNotFoundError:
        # Ubuntu 24.04 normally has coreutils. Keep a conservative fallback
        # for stripped-down installations; os.sync() flushes all filesystems.
        os.sync()
        return
    except subprocess.TimeoutExpired as exc:
        raise OSError("таймаут синхронизации USB-носителя") from exc

    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        if detail:
            raise OSError(f"ошибка синхронизации USB-носителя: {detail}")
        raise OSError("ошибка синхронизации USB-носителя")


def _verify_export_files(final_dir: Path, local_dir: Path) -> None:
    expected = {
        source.name: int(source.stat().st_size)
        for source in local_dir.iterdir()
        if source.is_file()
    }
    if not expected:
        raise OSError("локальный экспорт не содержит файлов")

    for name, expected_size in expected.items():
        target = final_dir / name
        try:
            actual_size = int(target.stat().st_size)
        except FileNotFoundError as exc:
            raise OSError(f"после синхронизации отсутствует файл {name}") from exc
        if actual_size != expected_size:
            raise OSError(
                f"размер файла {name} после записи не совпадает: "
                f"ожидалось {expected_size}, получено {actual_size}"
            )


def _copy_export_to_usb(*, local_dir: Path, drive: dict, final_name: str, required_bytes: int) -> Path:
    # Re-resolve and re-check immediately before touching the selected medium.
    current = _resolve_drive(drive["id"])
    mount_path = Path(current["mount_path"]).resolve(strict=True)
    if not current.get("writable"):
        reason = str(current.get("read_only_reason") or "").strip()
        if reason == "filesystem":
            raise OSError(
                "файловая система USB смонтирована только для чтения; "
                "возможна ошибка файловой системы после некорректного извлечения"
            )
        if reason == "device":
            raise OSError("USB-устройство отмечено системой как физически доступное только для чтения")
        raise OSError("нет прав на запись в точку монтирования USB-носителя")
    if not os.path.ismount(str(mount_path)):
        raise OSError("USB-носитель больше не смонтирован")

    vfs = os.statvfs(str(mount_path))
    free_bytes = int(vfs.f_bavail) * int(vfs.f_frsize)
    # Small reserve protects against metadata/filesystem overhead.
    reserve = max(1024 * 1024, int(required_bytes * 0.05))
    if free_bytes < required_bytes + reserve:
        raise OSError(
            f"Недостаточно свободного места: требуется не менее "
            f"{_human_size(required_bytes + reserve)}, доступно {_human_size(free_bytes)}"
        )

    tmp_name = EXPORT_TEMP_PREFIX + final_name[len(EXPORT_DIR_PREFIX):]
    tmp_dir = mount_path / tmp_name
    final_dir = mount_path / final_name
    if final_dir.exists() or tmp_dir.exists():
        raise OSError("Каталог экспорта с таким именем уже существует")

    renamed = False
    try:
        tmp_dir.mkdir(mode=0o755)
        for source in sorted(local_dir.iterdir()):
            if not source.is_file():
                continue
            target = tmp_dir / source.name
            with source.open("rb") as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
                dst.flush()
                os.fsync(dst.fileno())

        # If the medium disappeared, rename will fail and the request becomes
        # EXPORT_FAILED without touching any production state.
        tmp_dir.rename(final_dir)
        renamed = True

        # Best-effort directory fsync first. Some FAT/exFAT implementations may
        # reject directory fsync, therefore the filesystem-wide sync below is
        # mandatory and its errors are NOT ignored.
        try:
            dir_fd = os.open(str(final_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

        _sync_usb_filesystem(mount_path)
        _verify_export_files(final_dir, local_dir)
        return final_dir
    except Exception:
        _safe_cleanup(final_dir if renamed else tmp_dir)
        raise


def _unmount_usb_drive(drive: dict) -> None:
    mount_path = Path(str(drive.get("mount_path") or "")).resolve(strict=True)
    device = str(drive.get("device") or "").strip()
    if not device.startswith("/dev/"):
        raise OSError("для этого носителя автоматическое безопасное извлечение недоступно")
    if not os.path.ismount(str(mount_path)):
        return

    # Flush first even if the filesystem has already been remounted read-only.
    _sync_usb_filesystem(mount_path)

    attempts = [
        ["udisksctl", "unmount", "-b", device, "--no-user-interaction"],
        ["umount", str(mount_path)],
    ]
    errors: list[str] = []
    for command in attempts:
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=30,
                check=False,
            )
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            errors.append(f"{' '.join(command[:1])}: таймаут")
            continue

        if proc.returncode == 0:
            return
        detail = (proc.stderr or proc.stdout or "").strip()
        errors.append(f"{command[0]}: {detail or 'ошибка размонтирования'}")

    if errors:
        raise OSError("; ".join(errors))
    raise OSError("в системе отсутствуют udisksctl/umount для безопасного размонтирования")


@router.get("/drives")
def list_export_drives(
    _current_user: UserRow = Depends(require_admin),
):
    return {"drives": discover_usb_drives()}


@router.post("/unmount")
def unmount_export_drive(
    body: ExportUnmount,
    current_user: UserRow = Depends(require_admin),
    db: Session = Depends(_get_db),
):
    drive: dict | None = None
    try:
        drive = _resolve_drive(body.drive_id)
        _unmount_usb_drive(drive)
        repo.add_user_audit_event(
            db,
            type_=EventType.USB_UNMOUNTED.value,
            user_id=int(current_user.id),
            display_name=str(current_user.display_name),
            role=str(current_user.role),
            target={
                "type": "usb_drive",
                "drive_id": str(body.drive_id),
                "device": drive.get("device"),
                "mount_path": drive.get("mount_path"),
            },
            result="success",
            details={"message": "USB safely unmounted"},
        )
        return {
            "ok": True,
            "device": drive.get("device"),
            "mount_path": drive.get("mount_path"),
        }
    except HTTPException as exc:
        repo.add_user_audit_event(
            db,
            type_=EventType.USB_UNMOUNT_FAILED.value,
            user_id=int(current_user.id),
            display_name=str(current_user.display_name),
            role=str(current_user.role),
            target={
                "type": "usb_drive",
                "drive_id": str(body.drive_id),
                "device": None if drive is None else drive.get("device"),
                "mount_path": None if drive is None else drive.get("mount_path"),
            },
            result="failed",
            details={"error": str(exc.detail)},
            severity=Severity.error.value,
        )
        raise
    except Exception as exc:
        repo.add_user_audit_event(
            db,
            type_=EventType.USB_UNMOUNT_FAILED.value,
            user_id=int(current_user.id),
            display_name=str(current_user.display_name),
            role=str(current_user.role),
            target={
                "type": "usb_drive",
                "drive_id": str(body.drive_id),
                "device": None if drive is None else drive.get("device"),
                "mount_path": None if drive is None else drive.get("mount_path"),
            },
            result="failed",
            details={"error": str(exc)},
            severity=Severity.error.value,
        )
        raise HTTPException(
            status_code=409,
            detail=f"Не удалось безопасно размонтировать USB: {exc}",
        ) from exc


@router.post("")
def create_export(
    body: ExportCreate,
    current_user: UserRow = Depends(require_admin),
    db: Session = Depends(_get_db),
):
    drive: dict | None = None
    try:
        drive = _resolve_drive(body.drive_id)
        if not drive.get("writable"):
            raise HTTPException(
                status_code=409,
                detail="Выбранный USB-носитель доступен только для чтения.",
            )

        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        final_name = f"{EXPORT_DIR_PREFIX}{stamp}"

        with tempfile.TemporaryDirectory(prefix="postamats-export-") as temp_dir:
            local_dir = Path(temp_dir)
            stats = _build_local_export(db, local_dir)
            final_dir = _copy_export_to_usb(
                local_dir=local_dir,
                drive=drive,
                final_name=final_name,
                required_bytes=int(stats["bytes"]),
            )

        repo.add_user_audit_event(
            db,
            type_=EventType.EXPORT_CREATED.value,
            user_id=int(current_user.id),
            display_name=str(current_user.display_name),
            role=str(current_user.role),
            target={
                "type": "usb_export",
                "drive_id": drive["id"],
                "device": drive.get("device"),
                "mount_path": drive.get("mount_path"),
            },
            result="success",
            details={
                "directory": final_name,
                "batches_count": int(stats["batches_count"]),
                "events_count": int(stats["events_count"]),
                "bytes": int(stats["bytes"]),
            },
        )
        return {
            "ok": True,
            "directory": final_name,
            "path": str(final_dir),
            "batches_count": int(stats["batches_count"]),
            "events_count": int(stats["events_count"]),
            "bytes": int(stats["bytes"]),
            "bytes_human": _human_size(int(stats["bytes"])),
        }
    except HTTPException as exc:
        repo.add_user_audit_event(
            db,
            type_=EventType.EXPORT_FAILED.value,
            user_id=int(current_user.id),
            display_name=str(current_user.display_name),
            role=str(current_user.role),
            target={
                "type": "usb_export",
                "drive_id": str(body.drive_id),
                "device": None if drive is None else drive.get("device"),
                "mount_path": None if drive is None else drive.get("mount_path"),
            },
            result="failed",
            details={"error": str(exc.detail)},
            severity=Severity.error.value,
        )
        raise
    except Exception as exc:
        message = f"Не удалось выполнить экспорт: {exc}"
        repo.add_user_audit_event(
            db,
            type_=EventType.EXPORT_FAILED.value,
            user_id=int(current_user.id),
            display_name=str(current_user.display_name),
            role=str(current_user.role),
            target={
                "type": "usb_export",
                "drive_id": str(body.drive_id),
                "device": None if drive is None else drive.get("device"),
                "mount_path": None if drive is None else drive.get("mount_path"),
            },
            result="failed",
            details={"error": str(exc)},
            severity=Severity.error.value,
        )
        raise HTTPException(status_code=500, detail=message) from exc
