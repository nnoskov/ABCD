import os
import asyncio
import time
import re

from sqlalchemy import select

from app.infra.db import SessionLocal, init_sqlite_pragmas, engine
from app.infra.models import Base, CommandRow
from app.infra import repo
from app.daemon.supervisor import Supervisor
from app.daemon.utils import parse_product_name_and_spec
from app.daemon.io_factory import make_io
from app.daemon.printer import MockPrinter, Printer
from app.daemon.rtk_factory import make_rtk
from app.daemon.im_io import ImOpcUaIO, ImNodes
from app.common.enums import CommandType, CommandStatus, EventType, SystemMode, Severity
from app.common.timeutils import utcnow
from app.common.product_rules import (
    ProductRuleError,
    resolve_product_rule,
    uses_special_unloading_cell,
)

IM_WORKFLOW_MESSAGE_PREFIX = "im.workflow"
QUALITY_CALIBRATION_MESSAGE_PREFIX = "quality.calibration"


TRANSIENT_INFO_MESSAGE_TTL_SEC = 8.0
TRANSIENT_WARN_MESSAGE_TTL_SEC = 12.0
OPERATOR_MESSAGE_CLEANUP_PERIOD_SEC = 1.0


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


async def main_async():
    init_sqlite_pragmas()
    Base.metadata.create_all(bind=engine)

    reject_capacity = int(os.getenv("REJECT_BIN_CAPACITY", "50"))

    io = make_io()

    # printer_name = os.getenv("PRINTER", "")
    # spool_dir = os.getenv("PRINTER_SPOOL_DIR", "/tmp/postamat_print")
    # use_real = os.getenv("USE_REAL_PRINTER", "0") == "1" or bool(printer_name)
    # printer = Printer(printer_name=printer_name, spool_dir=spool_dir) if use_real else MockPrinter()

    rtk = make_rtk()
    sup = Supervisor(io=io, rtk=rtk, reject_bin_capacity=reject_capacity)

    async def _maybe_await(v):
        """Allow calling sync or async backends with the same code path."""
        if asyncio.iscoroutine(v):
            return await v
        return v

    def _set_message(
        db,
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

        Старые вызовы без message_key продолжают работать через общий
        workflow-ключ process.notice. Дополнительные поля состояния
        применяются отдельно и не попадают в запись сообщения.
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
        db,
        message: str,
        severity: str | Severity = Severity.info.value,
        *,
        message_key: str = "notice.general",
        ttl_sec: float | None = None,
        **state,
    ):
        """Публикует краткое подтверждение без изменения производственной логики."""
        severity_value = severity.value if hasattr(severity, "value") else str(severity)

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

    def _ui_message(
        db, message: str, severity: str | Severity = Severity.info.value, **state
    ):
        """
        Сообщение для интерфейса, которое должно стать видимым сразу,
        даже если дальше внутри async-команды будет sleep/retry.
        """
        _set_message(db, message, severity, **state)
        try:
            db.commit()
        except Exception:
            db.rollback()
            raise

    def _clear_resolved_command_error(
        db,
        *,
        successful_command: CommandRow | None = None,
    ) -> bool:
        """
        Снимает command.error только после подтверждённого успешного
        выполнения повторной команды того же типа.

        Без successful_command выполняется восстановительная проверка
        истории команд после рестарта daemon.
        """
        latest_failed = db.execute(
            select(CommandRow)
            .where(
                CommandRow.status == CommandStatus.failed.value,
            )
            .order_by(CommandRow.id.desc())
            .limit(1)
        ).scalar_one_or_none()

        if latest_failed is None:
            return False

        if successful_command is not None:
            if int(successful_command.id) <= int(latest_failed.id) or str(
                successful_command.type
            ) != str(latest_failed.type):
                return False
        else:
            successful_command = db.execute(
                select(CommandRow)
                .where(
                    CommandRow.status == CommandStatus.done.value,
                    CommandRow.type == str(latest_failed.type),
                    CommandRow.id > int(latest_failed.id),
                )
                .order_by(CommandRow.id.asc())
                .limit(1)
            ).scalar_one_or_none()

            if successful_command is None:
                return False

        return repo.clear_operator_message(
            db,
            "command.error",
        )

    def _connected_attr(obj, default: bool = True) -> bool:
        attr = getattr(obj, "connected", None)
        if attr is None:
            return bool(default)
        try:
            return bool(attr() if callable(attr) else attr)
        except Exception:
            return False

    IM_RECONNECT_SETTLE_SEC = float(os.getenv("IM_RECONNECT_SETTLE_SEC", "6.0"))
    IM_READY_AT_SETTING = "im_ready_at_ts"

    def _float_setting(db, name: str, default: float = 0.0) -> float:
        try:
            return float(repo.get_setting(db, name, default) or default)
        except Exception:
            return float(default)

    def _bool_setting(
        db,
        name: str,
        default: bool = False,
    ) -> bool:
        value = repo.get_setting(db, name, default)

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        normalized = str(value).strip().lower()

        if normalized in {"1", "true", "yes", "y", "on"}:
            return True

        if normalized in {"0", "false", "no", "n", "", "none", "null"}:
            return False

        return bool(default)

    def _im_settle_remaining_sec(db, *, now_ts: float | None = None) -> int:
        if im is None:
            return 0

        if now_ts is None:
            now_ts = time.time()

        ready_at = _float_setting(db, IM_READY_AT_SETTING, 0.0)
        left = float(ready_at) - float(now_ts)

        if left <= 0:
            return 0

        return int(left + 0.999)

    def _last_error_attr(obj) -> str:
        try:
            err = getattr(obj, "last_error", None)
            err = err() if callable(err) else err
            return str(err) if err else ""
        except Exception:
            return ""

    def _short_err(e: object) -> str:
        return str(e)[:500]

    def _operator_command_error_message(
        cmd_type: str,
        error: object,
    ) -> str:
        """
        Возвращает операторский текст, не изменяя техническую ошибку команды.

        Исходный error по-прежнему сохраняется в CommandRow.error и events.
        """
        raw = str(error or "").strip()
        if not raw:
            return "Команда не выполнена"

        exact = {
            "rtk not connected": "Нет связи с РТК",
            "reject bin is full": "Тара брака заполнена",
            "safety tripped": "Нарушен аварийный контур",
            "trash missing": "Отсутствует тара брака",
            "payload.batch_id is required": "Не указан номер партии",
            "payload.idx is required": "Не указан номер ячейки",
            "payload.product_code is required": "Не указано обозначение детали",
            "result is required": "Не получен результат операции",
            "missing fields for RTK_START": (
                "Не заполнены обязательные данные для запуска партии на РТК"
            ),
            "Layout must be int: 0, 1, 2 or 3": (
                "Раскладка должна быть выбрана из вариантов А, Б, В или Г"
            ),
            "Layout must be 0, 1, 2 or 3": (
                "Раскладка должна быть выбрана из вариантов А, Б, В или Г"
            ),
            "UseAlternateWave must be bool": ("Некорректно задан параметр «Волна»"),
            "payload.manual должен иметь тип bool": (
                "Некорректно выбран режим управления РТК"
            ),
            "нет давления воздуха": "Нет давления воздуха",
            "IM not configured (OPCUA_ENDPOINT_IM/IM_ENDPOINT is empty)": (
                "Микрометр не настроен"
            ),
            "IM_NODE_CLEAR_DB is not configured": (
                "Не настроена команда очистки журнала микрометра"
            ),
        }

        if raw in exact:
            return exact[raw]

        batch_not_found = re.fullmatch(r"batch\s+(\d+)\s+not found", raw)
        if batch_not_found:
            return f"Партия {batch_not_found.group(1)} не найдена"

        batch_not_found_alt = re.fullmatch(r"batch not found:\s*(\d+)", raw)
        if batch_not_found_alt:
            return f"Партия {batch_not_found_alt.group(1)} не найдена"

        unknown_command = re.fullmatch(
            r"unknown command type:\s*(.+)",
            raw,
        )
        if unknown_command:
            return "Получена неизвестная команда: " f"{unknown_command.group(1)}"

        unknown_im_command = re.fullmatch(
            r"unknown IM command:\s*(.+)",
            raw,
        )
        if unknown_im_command:
            return (
                "Получена неизвестная команда микрометра: "
                f"{unknown_im_command.group(1)}"
            )

        if re.search(r"[А-Яа-яЁё]", raw):
            return raw

        command_names = {
            CommandType.START_AUTO.value: "Запуск автоматического режима",
            CommandType.STOP_AUTO.value: "Остановка автоматического режима",
            CommandType.PAUSE_SYSTEM.value: "Пауза",
            CommandType.RESUME_SYSTEM.value: "Продолжение работы",
            CommandType.RTK_CYCLE_ON.value: "Запуск цикла РТК",
            CommandType.RTK_START.value: "Запуск партии на РТК",
            CommandType.RTK_SET_STEP_MODE.value: "Выбор режима РТК",
            CommandType.RTK_NEXT_STEP.value: "Следующий шаг РТК",
            CommandType.PRINT_DOCS.value: "Печать документов",
            CommandType.IM_LOAD_PROGRAM.value: "Загрузка программы микрометра",
            CommandType.IM_CALIBRATE.value: "Калибровка микрометра",
            CommandType.IM_MEASURE_ONCE.value: "Измерение на микрометре",
        }
        command_name = command_names.get(
            str(cmd_type),
            "Команда",
        )

        return (
            f"Не удалось выполнить операцию «{command_name}». "
            "Подробности записаны в журнал событий"
        )

    def _command_uses_modal_only_feedback(
        cmd_type: str,
        payload: dict | None,
    ) -> bool:
        """
        Команды процедуры извлечения возвращают результат только в модалку.

        Их успехи и ошибки остаются в CommandRow/events, но не публикуются
        в общем реестре сообщений оператора.
        """
        command_type = str(cmd_type or "")
        command_payload = payload or {}
        purpose = str(command_payload.get("purpose") or "").strip().lower()

        if command_type in {
            CommandType.CHECK_BATCH_EXTRACTION_DOORS.value,
            CommandType.MARK_ACTIVE_BATCH_EXTRACTED.value,
        }:
            return True

        if command_type in {
            CommandType.POSTAMAT_OPEN_LOADING_CELL.value,
            CommandType.POSTAMAT_OPEN_UNLOADING_CELL.value,
            CommandType.PRINT_DOCS.value,
        }:
            return purpose == "extraction"

        return False

    def _clear_modal_feedback_duplicate(
        db,
        *,
        error_text: str,
        operator_text: str,
    ) -> None:
        """
        Убирает только точные дубли старой модальной ошибки.

        Независимое command.error другой команды не затрагивается.
        """
        candidates = {
            str(error_text or "").strip(),
            str(operator_text or "").strip(),
        }
        candidates.discard("")

        if not candidates:
            return

        active = {
            str(item.get("key") or ""): item for item in repo.list_operator_messages(db)
        }

        for message_key in ("command.error", "process.notice"):
            current = active.get(message_key)
            if (
                current is not None
                and str(current.get("message") or "").strip() in candidates
            ):
                repo.clear_operator_message(db, message_key)

    def _guard_current_operation(
        db,
        *,
        where: str,
        batch_id=None,
        operation_id=None,
    ) -> bool:
        """
        True  - команда относится к текущей операции или это ручная команда вне операции.
        False - stale-команда старой операции; её надо тихо игнорировать.
        """
        st = repo.ensure_state_row(db)

        op_id = str(operation_id or "").strip()
        has_payload_context = batch_id is not None or bool(op_id)
        has_active_context = bool(
            str(getattr(st, "active_operation_id", "") or "").strip()
        )

        # Ручной/debug-сценарий вне автооперации.
        if not has_payload_context and not has_active_context:
            return True

        if repo.active_operation_matches(
            db,
            batch_id=batch_id,
            operation_id=op_id,
        ):
            return True

        repo.add_event(
            db,
            severity=Severity.warn.value,
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "message": "stale IM operation ignored",
                "where": where,
                "payload_batch_id": batch_id,
                "payload_operation_id": op_id or None,
                "active_batch_id": getattr(st, "active_batch_id", None),
                "active_operation_id": getattr(st, "active_operation_id", None),
                "active_operation_batch_id": getattr(
                    st, "active_operation_batch_id", None
                ),
            },
        )
        return False

    def _rtk_waiting_for_external_result() -> bool:
        try:
            snap = rtk.snapshot()
            a2 = (
                str(getattr(snap, "action_r2", "") or getattr(snap, "state", "") or "")
                .strip()
                .lower()
            )
            return a2 in {"waitingmmresult", "waitingcalibrationresult"}
        except Exception:
            return False

    def _air_pressure_ok_from_snapshot(snap) -> bool:
        if snap is None or not bool(getattr(snap, "connected", False)):
            return True

        v = getattr(snap, "air_pressure_ok", True)

        if v is None:
            return True

        return bool(v)

    # IM OPCUA (второй сервер)
    im_endpoint = os.getenv("OPCUA_ENDPOINT_IM") or os.getenv("IM_ENDPOINT") or ""
    im: ImOpcUaIO | None = None
    if im_endpoint:
        im = ImOpcUaIO(endpoint=im_endpoint, nodes=ImNodes.from_env())

    # startup: ни один внешний компонент не должен валить запуск daemon
    if hasattr(io, "connect"):
        try:
            await io.connect()
        except Exception as e:
            dbx = SessionLocal()
            try:
                err = _short_err(e)
                repo.set_setting(dbx, "postamat_connected", False)
                repo.set_setting(dbx, "postamat_error", err)
                _set_message(
                    dbx,
                    f"Постаматы: потеря связи ({err})",
                    Severity.error,
                    message_key="equipment.postamat",
                    safety_ok=0,
                    trash_present=0,
                )
                repo.add_event(
                    dbx,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={"where": "POSTAMAT_CONNECT", "err": err},
                )
            finally:
                dbx.close()

    # RTK connect
    if hasattr(rtk, "connect"):
        try:
            await _maybe_await(rtk.connect())
        except Exception as e:
            dbx = SessionLocal()
            try:
                err = _short_err(e)
                repo.set_setting(dbx, "rtk_connected", False)
                repo.set_setting(dbx, "rtk_error", err)
                _set_message(
                    dbx,
                    f"РТК: потеря связи ({err})",
                    Severity.error,
                    message_key="equipment.rtk",
                    rtk_connected=0,
                    rtk_error=err,
                )
                repo.add_event(
                    dbx,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={"where": "RTK_CONNECT", "err": err},
                )
            finally:
                dbx.close()

    if im is not None:
        try:
            await im.connect()
        except Exception as e:
            dbx = SessionLocal()
            try:
                err = _short_err(e)
                repo.set_setting(dbx, "im_connected", False)
                repo.set_setting(dbx, "im_error", err)
                repo.set_setting(dbx, "im_loaded_program", "")
                _set_message(
                    dbx,
                    f"Микрометр: потеря связи ({err})",
                    Severity.error,
                    message_key="equipment.im",
                )
                repo.add_event(
                    dbx,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={"where": "IM_CONNECT", "err": err},
                )
            finally:
                dbx.close()

    db = SessionLocal()
    try:
        sup.on_start(db)

        # Миграционная очистка этапа 8. После разделения сообщений по
        # независимым ключам общий process.notice больше не используется.
        # Старые сообщения модалки извлечения также не должны возвращаться
        # в общий интерфейс после перезапуска daemon.
        repo.clear_operator_message(db, "process.notice")
        repo.clear_operator_message(db, "notice.extraction")
        repo.clear_operator_message(db, "notice.print")

        latest_failed = db.execute(
            select(CommandRow)
            .where(CommandRow.status == CommandStatus.failed.value)
            .order_by(CommandRow.id.desc())
            .limit(1)
        ).scalar_one_or_none()

        if latest_failed is not None and _command_uses_modal_only_feedback(
            latest_failed.type,
            latest_failed.payload or {},
        ):
            _clear_modal_feedback_duplicate(
                db,
                error_text=str(latest_failed.error or ""),
                operator_text=_operator_command_error_message(
                    latest_failed.type,
                    latest_failed.error,
                ),
            )

        # Если daemon был перезапущен после ошибки публикации или между
        # неуспешной и успешной повторной командой, восстанавливаем
        # command.error по уже сохранённой истории CommandRow.
        try:
            if not (
                latest_failed is not None
                and _command_uses_modal_only_feedback(
                    latest_failed.type,
                    latest_failed.payload or {},
                )
            ):
                _clear_resolved_command_error(db)
        except Exception as message_exc:
            try:
                db.rollback()
            except Exception:
                pass

            try:
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "COMMAND_ERROR_MESSAGE_RECOVER",
                        "err": _short_err(message_exc),
                    },
                )
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
    finally:
        db.close()

    im_tasks: dict[str, asyncio.Task] = {}

    im_command_types = {
        CommandType.IM_CLEAR_DB.value,
        CommandType.IM_VACUUM_ON.value,
        CommandType.IM_VACUUM_OFF.value,
        CommandType.IM_LOAD_PROGRAM.value,
        CommandType.IM_CALIBRATE.value,
        CommandType.IM_MEASURE_ONCE.value,
    }

    def _im_failure_has_dedicated_equipment_message(
        cmd_type: str,
    ) -> bool:
        """
        True, если ошибка IM-команды вызвана фактической потерей связи.

        Такие ошибки уже публикуются местом возникновения под sticky-ключом
        equipment.im. Дублировать тот же текст через command.error нельзя:
        equipment.im снимается после восстановления связи, а command.error
        по общему правилу ждал бы успешной команды того же типа и оставался
        устаревшим до следующей партии.
        """
        return bool(
            str(cmd_type) in im_command_types
            and im is not None
            and not _connected_attr(im, default=False)
        )

    def _clear_duplicate_command_error(
        db,
        *,
        error_text: str,
    ) -> bool:
        """
        Удаляет только точный дубль equipment.im из command.error.

        Чужую ошибку команды не трогаем: это важно, если до потери связи
        уже существовал независимый command.error другого типа.
        """
        current = next(
            (
                item
                for item in repo.list_operator_messages(db)
                if str(item.get("key") or "") == "command.error"
            ),
            None,
        )

        if current is None or str(current.get("message") or "") != str(error_text):
            return False

        return repo.clear_operator_message(db, "command.error")

    def _rejectbin_command_uses_workflow_message(
        db,
        *,
        cmd_type: str,
    ) -> bool:
        """
        Ошибки команд процедуры замены тары не должны дублироваться
        общим sticky-ключом command.error.

        REPLACE_REJECTBIN публикует причину под reject_bin.workflow
        непосредственно в Supervisor. Неуспешный RESUME во время
        paused_rejectbin также относится к текущему workflow: его текст
        остаётся в CommandRow.error/событиях, но не создаёт второй баннер.
        """
        if str(cmd_type) == CommandType.REPLACE_REJECTBIN.value:
            return True

        if str(cmd_type) != CommandType.RESUME_SYSTEM.value:
            return False

        st = repo.ensure_state_row(db)
        return str(getattr(st, "mode", "") or "") == SystemMode.paused_rejectbin.value

    equip_prev: dict[str, bool | None] = {
        "postamat": None,
        "im": None,
        "rtk": None,
    }

    equipment_pause_active = False

    async def _publish_equipment_status(db):
        nonlocal equipment_pause_active

        postamat_ok = _connected_attr(io, default=True)
        im_ok = True if im is None else _connected_attr(im, default=False)

        snap = rtk.snapshot() if hasattr(rtk, "snapshot") else None
        rtk_ok = bool(getattr(snap, "connected", False)) if snap is not None else True

        now_ts = time.time()

        postamat_err = _last_error_attr(io)
        im_err = "" if im is None else _last_error_attr(im)
        rtk_err = str(getattr(snap, "error", "") or "") if snap is not None else ""

        repo.set_setting(db, "postamat_connected", bool(postamat_ok))
        repo.set_setting(db, "postamat_error", postamat_err or "")

        repo.set_setting(db, "im_connected", bool(im_ok))
        repo.set_setting(db, "im_error", im_err or "")
        if not im_ok:
            # После offline/зависания Микрометра программа могла сброситься.
            # Старому settings.im_loaded_program больше не доверяем.
            repo.set_setting(db, "im_loaded_program", "")

        repo.set_setting(db, "rtk_connected", bool(rtk_ok))
        repo.set_setting(db, "rtk_error", rtk_err or "")
        repo.set_setting(
            db, "air_pressure_ok", bool(_air_pressure_ok_from_snapshot(snap))
        )

        labels = {
            "postamat": "Постаматы",
            "im": "Микрометр",
            "rtk": "РТК",
        }
        values = {
            "postamat": bool(postamat_ok),
            "im": bool(im_ok),
            "rtk": bool(rtk_ok),
        }
        errors = {
            "postamat": postamat_err,
            "im": im_err,
            "rtk": rtk_err,
        }
        message_keys = {
            "postamat": "equipment.postamat",
            "im": "equipment.im",
            "rtk": "equipment.rtk",
        }

        active_messages = {
            str(item.get("key") or ""): item for item in repo.list_operator_messages(db)
        }

        def _ensure_equipment_message(
            message_key: str,
            message: str,
            severity: str | Severity,
        ) -> None:
            severity_value = (
                severity.value if hasattr(severity, "value") else str(severity)
            )
            current = active_messages.get(message_key)

            # Более конкретный текст, опубликованный местом фактической ошибки
            # (например, ожидание результата ИМ), не перетираем общей проверкой
            # соединений на каждом проходе main loop.
            if (
                current is not None
                and str(current.get("severity") or "") == severity_value
            ):
                return

            stored = _set_message(
                db,
                message,
                severity_value,
                message_key=message_key,
            )
            active_messages[message_key] = stored

        # События пишем только на переходах. Сам реестр сообщений ниже
        # синхронизируется с физическим состоянием на каждом проходе, поэтому
        # после рестарта daemon не останется устаревшего sticky-сообщения.
        for key, ok_now in values.items():
            ok_prev = equip_prev.get(key)

            if ok_prev is True and not ok_now:
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "EQUIPMENT_CONNECTION_LOST",
                        "equipment": key,
                        "err": errors.get(key) or "",
                    },
                )

                if key == "im":
                    repo.set_setting(db, IM_READY_AT_SETTING, 0.0)
                    repo.set_setting(db, "im_loaded_program", "")

            elif ok_prev is False and ok_now:
                if key == "im":
                    ready_at = now_ts + float(IM_RECONNECT_SETTLE_SEC)
                    repo.set_setting(db, IM_READY_AT_SETTING, float(ready_at))
                    repo.set_setting(db, "im_loaded_program", "")

                    repo.add_event(
                        db,
                        severity=Severity.warn.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={
                            "message": "Микрометр: связь восстановлена; ожидание стабилизации",
                            "equipment": key,
                            "im_ready_at_ts": float(ready_at),
                            "settle_sec": float(IM_RECONNECT_SETTLE_SEC),
                        },
                    )
                else:
                    repo.add_event(
                        db,
                        severity=Severity.info.value,
                        source="DAEMON",
                        type_=EventType.STATE_UPDATED.value,
                        payload={
                            "message": f"{labels[key]}: связь восстановлена",
                            "equipment": key,
                        },
                    )

            equip_prev[key] = bool(ok_now)

        im_settle_left = _im_settle_remaining_sec(db, now_ts=now_ts)
        im_settling = bool(im is not None and im_ok and im_settle_left > 0)

        # Независимые sticky-ключи оборудования. Восстановление одного
        # компонента удаляет только его сообщение и не затрагивает остальные.
        for key in ("postamat", "rtk"):
            if values[key]:
                repo.clear_operator_message(db, message_keys[key])
            else:
                msg = f"{labels[key]}: потеря связи"
                if errors.get(key):
                    msg += f" ({errors[key]})"
                _ensure_equipment_message(
                    message_keys[key],
                    msg,
                    Severity.error,
                )

        if im is None:
            repo.clear_operator_message(db, message_keys["im"])
        elif not im_ok:
            msg = "Микрометр: потеря связи"
            if im_err:
                msg += f" ({im_err})"
            _ensure_equipment_message(
                message_keys["im"],
                msg,
                Severity.error,
            )
        elif im_settling:
            _ensure_equipment_message(
                message_keys["im"],
                "Микрометр: связь восстановлена; "
                f"ожидание стабилизации {im_settle_left} сек.",
                Severity.warn,
            )
        else:
            repo.clear_operator_message(db, message_keys["im"])

        st = repo.ensure_state_row(db)
        any_bad = (not postamat_ok) or (not im_ok) or (not rtk_ok) or im_settling

        if any_bad:
            # Постаматы дополнительно приводят Supervisor к safety-паузе.
            # Для ИМ/РТК сохраняем автоматическую equipment-паузу, но если
            # одновременно потеряна связь с постаматами, не подменяем будущую
            # paused_safety промежуточным paused_operator.
            reasons = []
            if not postamat_ok:
                reasons.append("Постаматы")
            if not im_ok:
                reasons.append("Микрометр")
            elif im_settling:
                reasons.append(
                    f"Микрометр: стабилизация после восстановления ({im_settle_left} сек.)"
                )
            if not rtk_ok:
                reasons.append("РТК")

            requires_equipment_pause = (not im_ok) or (not rtk_ok) or im_settling
            saved_equipment_pause = bool(
                repo.get_setting(db, "equipment_pause_active", False)
            )
            active_auto_context = st.mode == SystemMode.auto_running.value or (
                str(st.mode or "").startswith("paused")
                and (
                    st.active_batch_id is not None or bool(getattr(st, "rtk_paused", 0))
                )
            )

            if requires_equipment_pause and (
                active_auto_context or equipment_pause_active or saved_equipment_pause
            ):
                equipment_pause_active = True
                repo.set_setting(db, "equipment_pause_active", True)
                repo.set_setting(db, "equipment_pause_reason", ",".join(reasons))

                if not saved_equipment_pause:
                    repo.set_setting(db, "equipment_pause_owns_mode", False)

                if active_auto_context:
                    repo.set_setting(
                        db,
                        "equipment_pause_target_mode",
                        SystemMode.auto_running.value,
                    )

                if st.mode == SystemMode.auto_running.value:
                    # При отсутствии постаматов safety-паузу и RTK pause
                    # выполнит Supervisor в этом же цикле.
                    if postamat_ok:
                        repo.set_setting(db, "equipment_pause_owns_mode", True)
                        # Если РТК уже ждёт результат Микрометра/калибровки,
                        # НЕ двигаем его pause-командой: сохраняем waiting*result.
                        if rtk_ok and not _rtk_waiting_for_external_result():
                            rtk.request(
                                "pause",
                                {"reason": "equipment_connection_lost"},
                            )
                            repo.set_state(
                                db,
                                rtk_paused=1,
                                rtk_pause_reason="equipment_connection_lost",
                            )
                            repo.add_event(
                                db,
                                severity=Severity.warn.value,
                                source="DAEMON",
                                type_=EventType.RTK_COMMAND_SENT.value,
                                payload={
                                    "cmd": "pause",
                                    "reason": "equipment_connection_lost",
                                },
                            )

                        repo.set_state(db, mode=SystemMode.paused_operator.value)
                        repo.add_event(
                            db,
                            severity=Severity.warn.value,
                            source="DAEMON",
                            type_=EventType.STATE_UPDATED.value,
                            payload={
                                "message": (
                                    "auto cycle paused because equipment "
                                    "is unavailable"
                                ),
                                "mode": SystemMode.paused_operator.value,
                                "equipment": list(reasons),
                            },
                        )

            return

        # Все соединения восстановились, включая период стабилизации ИМ.
        if im is not None and _float_setting(db, IM_READY_AT_SETTING, 0.0) > 0:
            repo.set_setting(db, IM_READY_AT_SETTING, 0.0)

        if equipment_pause_active or bool(
            repo.get_setting(db, "equipment_pause_active", False)
        ):
            equipment_pause_active = False
            repo.set_setting(db, "equipment_pause_active", False)
            repo.set_setting(db, "equipment_pause_reason", "")

            st = repo.ensure_state_row(db)
            target = str(repo.get_setting(db, "equipment_pause_target_mode", "") or "")
            owns_mode = bool(repo.get_setting(db, "equipment_pause_owns_mode", False))
            repo.set_setting(db, "equipment_pause_target_mode", "")
            repo.set_setting(db, "equipment_pause_owns_mode", False)

            if (
                owns_mode
                and st.mode == SystemMode.paused_operator.value
                and target == SystemMode.auto_running.value
            ):
                if bool(getattr(st, "rtk_paused", 0)) and rtk_ok:
                    rtk.request("resume", {"reason": "equipment_connection_restored"})
                    repo.set_state(db, rtk_paused=0, rtk_pause_reason=None)
                    repo.add_event(
                        db,
                        severity=Severity.info.value,
                        source="DAEMON",
                        type_=EventType.RTK_COMMAND_SENT.value,
                        payload={
                            "cmd": "resume",
                            "reason": "equipment_connection_restored",
                        },
                    )

                _set_transient_message(
                    db,
                    "Связь с оборудованием восстановлена; автоцикл продолжен",
                    Severity.info,
                    message_key="notice.system",
                    mode=SystemMode.auto_running.value,
                )

    async def _start_im_calibration(mode: int, *, batch_id=None, operation_id=None):
        assert im is not None

        mode = int(mode)
        if mode not in (1, 2):
            return

        workflow_message_key = (
            _batch_operator_message_key(
                IM_WORKFLOW_MESSAGE_PREFIX,
                batch_id,
            )
            or IM_WORKFLOW_MESSAGE_PREFIX
        )

        db_check = SessionLocal()
        try:
            if not _guard_current_operation(
                db_check,
                where="IM_CALIBRATE_TASK_START",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return
        finally:
            db_check.close()

        if not _connected_attr(im, default=False):
            db2 = SessionLocal()
            try:
                state_patch = {}
                if _guard_current_operation(
                    db2,
                    where="IM_CALIBRATE_START_OFFLINE",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    state_patch = {
                        "calibration_inflight": 0,
                        "calib_result_inflight": 0,
                    }

                _set_message(
                    db2,
                    "Микрометр: нет связи; повторная калибровка/проверка после восстановления связи",
                    Severity.error,
                    message_key="equipment.im",
                    **state_patch,
                )

                repo.set_setting(db2, "im_connected", False)
                repo.set_setting(
                    db2, "im_error", _last_error_attr(im) or "not connected"
                )
                repo.add_event(
                    db2,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_CALIBRATE_START",
                        "err": _last_error_attr(im) or "not connected",
                    },
                )
            finally:
                db2.close()
            return

        im.request_calibrate(mode)
        await im.poll_once()

        if not _connected_attr(im, default=False):
            db2 = SessionLocal()
            try:
                state_patch = {}
                if _guard_current_operation(
                    db2,
                    where="IM_CALIBRATE_START_OFFLINE",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    state_patch = {
                        "calibration_inflight": 0,
                        "calib_result_inflight": 0,
                    }

                _set_message(
                    db2,
                    "Микрометр: связь потеряна при отправке команды калибровки; повтор после восстановления",
                    Severity.error,
                    message_key="equipment.im",
                    **state_patch,
                )
                repo.set_setting(db2, "im_connected", False)
                repo.set_setting(
                    db2, "im_error", _last_error_attr(im) or "not connected"
                )
            finally:
                db2.close()
            return

        res = await im.wait_calibration_done(
            timeout_sec=300.0, poll_period_sec=0.2, stable_polls=3
        )

        if res is None:
            db2 = SessionLocal()
            try:
                state_patch = {}
                if _guard_current_operation(
                    db2,
                    where="IM_CALIBRATE_START_OFFLINE",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    state_patch = {
                        "calibration_inflight": 0,
                        "calib_result_inflight": 0,
                    }

                _set_message(
                    db2,
                    "Микрометр: калибровка/проверка не завершена; ждём восстановление связи или повтор команды",
                    Severity.error,
                    message_key=(
                        "equipment.im"
                        if not _connected_attr(im, default=False)
                        else workflow_message_key
                    ),
                    **state_patch,
                )
                repo.add_event(
                    db2,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_CALIBRATE_WAIT",
                        "err": "timeout_or_disconnected",
                    },
                )
            finally:
                db2.close()
            return

        # normalize to int result: 0 OK, -1 -2 -3 -4 NOK
        if isinstance(res, dict):
            res_code = int(res.get("calibration_result", res.get("result", 0)))
        else:
            res_code = int(res)

        db2 = SessionLocal()
        try:

            ok, err = sup.handle_command(
                db2,
                CommandType.IM_CALIBRATION_RESULT.value,
                {
                    "result": res_code,
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                },
            )
            if not ok:
                repo.add_event(
                    db2,
                    severity="error",
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={"where": "IM_CALIBRATE", "err": err},
                )
        finally:
            db2.close()

    async def _start_im_measure_once(
        product_code: str, *, batch_id=None, operation_id=None
    ):
        workflow_message_key = (
            _batch_operator_message_key(
                IM_WORKFLOW_MESSAGE_PREFIX,
                batch_id,
            )
            or IM_WORKFLOW_MESSAGE_PREFIX
        )

        db_check = SessionLocal()
        try:
            if not _guard_current_operation(
                db_check,
                where="IM_MEASURE_TASK_START",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return
        finally:
            db_check.close()

        assert im is not None

        if not _connected_attr(im, default=False):
            db2 = SessionLocal()
            try:
                state_patch = {}
                if _guard_current_operation(
                    db2,
                    where="IM_MEASURE_ERROR",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    state_patch = {"mm_result_inflight": 0}

                _set_message(
                    db2,
                    "Микрометр: нет связи, повтор измерения после восстановления",
                    Severity.error,
                    message_key="equipment.im",
                    **state_patch,
                )
                repo.set_setting(db2, "im_connected", False)
                repo.set_setting(
                    db2, "im_error", _last_error_attr(im) or "not connected"
                )
                repo.add_event(
                    db2,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_MEASURE_START",
                        "product_code": product_code,
                        "err": _last_error_attr(im) or "not connected",
                    },
                )
            finally:
                db2.close()
            return

        im.request_start_measurement()
        await im.poll_once()
        await asyncio.sleep(0.05)
        await im.poll_once()

        if not _connected_attr(im, default=False):
            db2 = SessionLocal()
            try:
                state_patch = {}
                if _guard_current_operation(
                    db2,
                    where="IM_MEASURE_ERROR",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    state_patch = {"mm_result_inflight": 0}

                _set_message(
                    db2,
                    "Микрометр: связь потеряна при запуске измерения, повтор после восстановления",
                    Severity.error,
                    message_key="equipment.im",
                    **state_patch,
                )
                repo.set_setting(db2, "im_connected", False)
                repo.set_setting(
                    db2, "im_error", _last_error_attr(im) or "not connected"
                )
            finally:
                db2.close()
            return

        values, tols = await im.wait_measure_done_and_read(
            product_code,
            timeout_sec=120.0,
            poll_period_sec=0.2,
        )

        if not values and not tols:
            db2 = SessionLocal()
            try:
                state_patch = {}
                if _guard_current_operation(
                    db2,
                    where="IM_MEASURE_ERROR",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    state_patch = {"mm_result_inflight": 0}

                _set_message(
                    db2,
                    "Микрометр: измерение не завершено или нет результатов; ждём восстановление связи или повтор команды",
                    Severity.error,
                    message_key=(
                        "equipment.im"
                        if not _connected_attr(im, default=False)
                        else workflow_message_key
                    ),
                    **state_patch,
                )
                repo.add_event(
                    db2,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_MEASURE_WAIT",
                        "product_code": product_code,
                        "err": "no_results_or_disconnected",
                    },
                )
            finally:
                db2.close()
            return

        db2 = SessionLocal()
        try:
            ok, err = sup.handle_command(
                db2,
                CommandType.IM_MEASUREMENT_RESULT.value,
                {
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                    "product_code": product_code,
                    "values": values,
                    "tols": tols,
                },
            )
            if not ok:
                repo.add_event(
                    db2,
                    severity="error",
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={"where": "IM_MEASURE_ONCE", "err": err},
                )
        finally:
            db2.close()

    async def maybe_start_next_auto_batch(db):
        st = repo.ensure_state_row(db)
        if st.mode != SystemMode.auto_running.value:
            return

        if bool(repo.get_setting(db, "equipment_pause_active", False)):
            return

        if st.active_batch_id is not None:
            return

        snap = rtk.snapshot() if hasattr(rtk, "snapshot") else None
        if (
            not snap
            or not getattr(snap, "connected", False)
            or getattr(snap, "busy", False)
        ):
            return

        postamat_ok = _connected_attr(io, default=True)
        im_ok = True if im is None else _connected_attr(im, default=False)
        rtk_ok = bool(getattr(snap, "connected", False))
        air_pressure_ok = _air_pressure_ok_from_snapshot(snap)
        im_settle_left = _im_settle_remaining_sec(db)

        if (
            (not postamat_ok)
            or (not im_ok)
            or (not rtk_ok)
            or (not air_pressure_ok)
            or im_settle_left > 0
        ):
            reasons = []

            if not postamat_ok:
                reasons.append("Постаматы")
            if not im_ok:
                reasons.append("Микрометр")
            elif im_settle_left > 0:
                reasons.append(
                    f"Микрометр: стабилизация после восстановления ({im_settle_left} сек.)"
                )
            if not rtk_ok:
                reasons.append("РТК")
            if not air_pressure_ok:
                reasons.append("Воздух")

            _set_message(
                db,
                "АВТОЦИКЛ: старт партии отложен - нет готовности оборудования: "
                + ", ".join(reasons),
                Severity.warn if im_settle_left > 0 else Severity.error,
                message_key="auto.start.blocked",
            )
            return

        repo.clear_operator_message(db, "auto.start.blocked")

        b = repo.claim_next_loaded_batch(db)
        if not b:
            return

        data = b.data or {}
        loc = b.location or {}

        raw_code = data.get("product_code") or data.get("product_name") or ""
        pn_base, pn_spec_from_code = parse_product_name_and_spec(str(raw_code))
        product_spec = int(data.get("product_spec") or pn_spec_from_code or 0)
        product_code_full = str(raw_code).strip() or pn_base
        if product_spec and "-" not in str(raw_code):
            product_code_full = f"{pn_base}-{product_spec:02d}"
        im_code = pn_base if product_spec != 0 else product_code_full
        product_count = int(data.get("product_count") or data.get("qty") or 0)

        try:
            layout = int(data.get("layout", 0))
        except (TypeError, ValueError):
            layout = 0

        if layout not in (0, 1, 2, 3):
            layout = 0

        if "use_alternate_wave" in data:
            use_alternate_wave = data.get("use_alternate_wave") is True
        else:
            use_alternate_wave = _bool_setting(
                db,
                "use_alternate_wave",
                False,
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
            _set_message(
                db,
                f"Партия {int(b.id)} не запущена: {error_message}",
                Severity.error,
                message_key=f"batch.start.{int(b.id)}",
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
                    "use_alternate_wave": bool(use_alternate_wave),
                    "product_count": int(product_count),
                    "err": error_message,
                },
            )
            return

        product_code_full = product_rule.product_code
        pn_base = product_rule.product_name
        product_spec = int(product_rule.product_spec)
        im_code = pn_base if product_spec != 0 else product_code_full

        in_ids = data.get("in_tare_ids") or loc.get("in_tare_ids") or []
        out_ids = data.get("out_tare_ids") or loc.get("out_tare_ids") or []

        cell_no = loc.get("cell_no") or data.get("cell_no")
        if cell_no is not None:
            if not in_ids:
                in_ids = [cell_no]
            if not out_ids:
                out_ids = [cell_no]

        if not isinstance(in_ids, list):
            in_ids = [in_ids]
        if not isinstance(out_ids, list):
            out_ids = [out_ids]

        try:
            in_ids = [int(x) for x in in_ids]
            out_ids = [int(x) for x in out_ids]
        except Exception:
            in_ids, out_ids = [], []

        if uses_special_unloading_cell(product_rule.product_code):
            out_ids = [16]

        if (not pn_base) or product_count <= 0 or not in_ids or not out_ids:
            repo.add_event(
                db,
                severity="error",
                source="DAEMON",
                type_=EventType.ERROR.value,
                payload={
                    "where": "AUTO_START_BATCH",
                    "err": "missing fields",
                    "batch_id": b.id,
                    "product_code": product_code_full,
                    "product_count": product_count,
                    "in_tare_ids": in_ids,
                    "out_tare_ids": out_ids,
                },
            )
            repo.set_batch_status(
                db, b.id, status="blocked", reject_reason="missing_fields_for_start"
            )
            _set_message(
                db,
                f"Партия {b.id} пропущена: не заполнены обязательные данные для запуска",
                Severity.error,
                message_key=f"batch.start.{int(b.id)}",
            )
            return

        # 1) IM load program
        ok, err = await handle_im_command(
            db, CommandType.IM_LOAD_PROGRAM.value, {"product_code": str(im_code)}
        )
        if not ok:
            repo.add_event(
                db,
                severity="error",
                source="DAEMON",
                type_=EventType.ERROR.value,
                payload={"where": "AUTO_START_IM_LOAD", "batch_id": b.id, "err": err},
            )

            if im is not None and not _connected_attr(im, default=False):
                repo.set_batch_status(db, b.id, status="loaded")
                _set_message(
                    db,
                    f"АВТОЦИКЛ: партия не стартовала — потеряна связь с микрометром при загрузке программы; повтор после восстановления",
                    Severity.error,
                    message_key=f"batch.start.{int(b.id)}",
                )
                return

            repo.set_batch_status(
                db,
                b.id,
                status="blocked",
                reject_reason=f"im_load_failed:{err}",
            )
            _set_message(
                db,
                f"АВТОЦИКЛ: не удалось загрузить программу для партии",
                Severity.error,
                message_key=f"batch.start.{int(b.id)}",
            )
            return

        # 2) RTK start
        start_payload = {
            "ProductName": str(pn_base),
            "ProductSpec": int(product_spec),
            "ProductCount": int(product_count),
            "InTareIDs": in_ids,
            "OutTareIDs": out_ids,
            "Layout": int(layout),
            "GlobalMaxTareCount": int(product_rule.global_max_tare_count),
            "CurrentMaxTareCount": int(product_rule.current_max_tare_count),
            "UseAlternateWave": bool(use_alternate_wave),
        }
        rtk.request("start", start_payload)
        repo.add_event(
            db,
            severity="info",
            source="DAEMON",
            type_=EventType.RTK_COMMAND_SENT.value,
            payload={"cmd": "start", "payload": start_payload, "batch_id": b.id},
        )

        repo.set_batch_status(db, b.id, status="auto_processing", loaded_at=utcnow())
        repo.clear_operator_message(db, f"batch.start.{int(b.id)}")
        _set_transient_message(
            db,
            f"АВТОЦИКЛ: партия запущена",
            Severity.info,
            message_key="notice.batch",
            active_batch_id=b.id,
            active_batch_phase="starting",
            active_batch_expected_count=int(product_count),
            rtk_pickcount_seen=None,
            rtk_defectcount_seen=None,
            rtk_consecutive_defects=0,
        )
        repo.add_event(
            db,
            severity="info",
            source="DAEMON",
            type_=EventType.STATE_UPDATED.value,
            payload={
                "active_batch_id": b.id,
                "active_batch_expected_count": int(product_count),
            },
        )

    async def _load_im_program_until_ok(
        db,
        product_code: str,
        *,
        batch_id=None,
        operation_id=None,
        after_calibration: bool = False,
        phase: str | None = None,
        delay_before_load_sec: float = 0.0,
        settle_after_load_sec: float = 0.0,
        next_calib_mode: int | None = None,
    ) -> tuple[bool, str | None]:
        """
        Загрузка измерительной программы в Микрометр(ИМ).

        Важное поведение:
        - если ИМ offline — НЕ блокируем основной daemon loop;
          возвращаем ошибку, UI получает сообщение, Supervisor сможет повторить команду
          после восстановления связи;
        - если ИМ online, но load_program вернул False — оставляем старую логику retry
          с сообщением оператору;
        - settings.im_loaded_program обновляется только после успешной загрузки.
        """
        assert im is not None

        pc = str(product_code).strip()
        if not pc:
            return False, "product_code is empty"

        workflow_message_key = (
            _batch_operator_message_key(
                IM_WORKFLOW_MESSAGE_PREFIX,
                batch_id,
            )
            or IM_WORKFLOW_MESSAGE_PREFIX
        )

        if not _guard_current_operation(
            db,
            where=f"IM_LOAD_PROGRAM:{phase or '-'}:start",
            batch_id=batch_id,
            operation_id=operation_id,
        ):
            return True, None

        try:
            mode_i = int(next_calib_mode or 0)
        except Exception:
            mode_i = 0

        op_name = None
        if mode_i == 1:
            op_name = "калибровка"
        elif mode_i == 2:
            op_name = "проверка"

        if delay_before_load_sec > 0:
            if after_calibration:
                msg = f"Микрометр: калибровка завершена; переключение программы {pc} через {delay_before_load_sec:.0f} сек."
            else:
                msg = f"Микрометр: переключение программы {pc} через {delay_before_load_sec:.0f} сек."

            _ui_message(
                db,
                msg,
                Severity.info,
                message_key=workflow_message_key,
            )
            await asyncio.sleep(float(delay_before_load_sec))

            if not _guard_current_operation(
                db,
                where=f"IM_LOAD_PROGRAM:{phase or '-'}:after_delay",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return True, None

        attempt = 0

        while True:
            attempt += 1

            # Если ИМ уже offline — не зависаем внутри команды.
            # Основной цикл должен продолжать опрашивать РТК/постаматы и обновлять UI.
            if not _connected_attr(im, default=False):
                try:
                    await im.poll_once()
                except Exception:
                    pass

            if not _connected_attr(im, default=False):
                err = _last_error_attr(im) or "not connected"
                msg = f"Микрометр: нет связи, программа {pc} не загружена ({err})"
                _ui_message(
                    db,
                    msg,
                    Severity.error,
                    message_key="equipment.im",
                )
                repo.set_setting(db, "im_connected", False)
                repo.set_setting(db, "im_error", err)
                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_LOAD_PROGRAM_OFFLINE",
                        "product_code": pc,
                        "attempt": int(attempt),
                        "batch_id": batch_id,
                        "phase": phase,
                        "err": err,
                    },
                )
                db.commit()
                return False, msg

            _ui_message(
                db,
                f"Микрометр: загрузка программы {pc}, попытка {attempt}",
                Severity.info,
                message_key=workflow_message_key,
            )

            try:
                if not _guard_current_operation(
                    db,
                    where=f"IM_LOAD_PROGRAM:{phase or '-'}:before_load",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    return True, None
                ok = await im.load_program(pc)
            except Exception as e:
                ok = False
                try:
                    await im.disconnect()
                except Exception:
                    pass

                err = str(e)
                msg = f"Микрометр: ошибка загрузки программы {pc} ({err})"

                _ui_message(
                    db,
                    msg,
                    Severity.error,
                    message_key="equipment.im",
                )
                repo.set_setting(db, "im_connected", False)
                repo.set_setting(db, "im_error", err)

                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_LOAD_PROGRAM_EXCEPTION",
                        "product_code": pc,
                        "attempt": int(attempt),
                        "batch_id": batch_id,
                        "phase": phase,
                        "err": err,
                    },
                )
                db.commit()

                return False, msg

            # Если load_program внутри поймал обрыв и вернул False,
            # надо отличить это от обычного "программа не загружена".
            if not _connected_attr(im, default=False):
                err = _last_error_attr(im) or "not connected"
                msg = f"Микрометр: связь потеряна при загрузке программы {pc} ({err})"

                _ui_message(
                    db,
                    msg,
                    Severity.error,
                    message_key="equipment.im",
                )
                repo.set_setting(db, "im_connected", False)
                repo.set_setting(db, "im_error", err)

                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_LOAD_PROGRAM_DISCONNECTED",
                        "product_code": pc,
                        "attempt": int(attempt),
                        "batch_id": batch_id,
                        "phase": phase,
                        "err": err,
                    },
                )
                db.commit()

                return False, msg

            repo.add_event(
                db,
                severity=(Severity.info.value if ok else Severity.warn.value),
                source="DAEMON",
                type_=EventType.IM_PROGRAM_LOADED.value,
                payload={
                    "product_code": pc,
                    "ok": bool(ok),
                    "attempt": int(attempt),
                    "after_calibration": bool(after_calibration),
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                    "phase": phase,
                    "next_calib_mode": mode_i,
                },
            )
            db.commit()

            if ok:
                if not _guard_current_operation(
                    db,
                    where=f"IM_LOAD_PROGRAM:{phase or '-'}:before_commit_loaded",
                    batch_id=batch_id,
                    operation_id=operation_id,
                ):
                    return True, None

                repo.set_setting(db, "im_loaded_program", pc)
                repo.set_setting(db, "im_connected", True)
                repo.set_setting(db, "im_error", "")

                if after_calibration and batch_id:
                    st = repo.ensure_state_row(db)
                    if int(st.active_batch_id or 0) == int(batch_id):
                        if (
                            int(getattr(st, "calib_wait_im_program", 0) or 0) == 1
                            and getattr(st, "calib_im_program_target", None) == pc
                        ):
                            repo.set_state(
                                db,
                                calib_wait_im_program=0,
                                calib_im_program_target=None,
                            )
                            repo.add_event(
                                db,
                                severity=Severity.info.value,
                                source="DAEMON",
                                type_=EventType.STATE_UPDATED.value,
                                payload={
                                    "message": f"IM program switched after calibration for batch {int(batch_id)}",
                                    "batch_id": int(batch_id),
                                    "im_program": pc,
                                },
                            )

                if settle_after_load_sec > 0:
                    if op_name:
                        _ui_message(
                            db,
                            f"Микрометр: программа {pc} загружена, попытка {attempt}; {op_name} через {settle_after_load_sec:.0f} сек.",
                            Severity.info,
                            message_key=workflow_message_key,
                        )
                    else:
                        _ui_message(
                            db,
                            f"Микрометр: программа {pc} загружена, попытка {attempt}; продолжение через {settle_after_load_sec:.0f} сек.",
                            Severity.info,
                            message_key=workflow_message_key,
                        )

                    await asyncio.sleep(float(settle_after_load_sec))

                    if not _guard_current_operation(
                        db,
                        where=f"IM_LOAD_PROGRAM:{phase or '-'}:after_settle",
                        batch_id=batch_id,
                        operation_id=operation_id,
                    ):
                        return True, None

                else:
                    _ui_message(
                        db,
                        f"Микрометр: программа {pc} загружена, попытка {attempt}",
                        Severity.info,
                        message_key=workflow_message_key,
                    )

                db.commit()
                return True, None

            _ui_message(
                db,
                f"Микрометр: программа {pc} не загружена, попытка {attempt}; повтор через 3 сек.",
                Severity.warn,
                message_key=workflow_message_key,
            )
            await asyncio.sleep(3.0)

    async def handle_im_command(
        db, cmd_type: str, payload: dict
    ) -> tuple[bool, str | None]:
        if im is None:
            return False, "IM not configured (OPCUA_ENDPOINT_IM/IM_ENDPOINT is empty)"

        async def _ensure_im_connected(operation: str) -> tuple[bool, str | None]:
            was_connected = _connected_attr(im, default=False)

            if not was_connected:
                try:
                    await im.poll_once()
                except Exception:
                    pass

            if not _connected_attr(im, default=False):
                err = _last_error_attr(im) or "not connected"
                msg = f"Микрометр: нет связи; команда {operation} невозможна сейчас ({err})"

                _ui_message(
                    db,
                    msg,
                    Severity.error,
                    message_key="equipment.im",
                )
                repo.set_setting(db, "im_connected", False)
                repo.set_setting(db, "im_error", err)
                repo.set_setting(db, "im_loaded_program", "")

                repo.add_event(
                    db,
                    severity=Severity.error.value,
                    source="DAEMON",
                    type_=EventType.ERROR.value,
                    payload={
                        "where": "IM_COMMAND_OFFLINE",
                        "cmd": operation,
                        "err": err,
                    },
                )
                return False, msg

            settle_left = _im_settle_remaining_sec(db)

            if settle_left > 0:
                msg = (
                    "Микрометр: связь восстановлена; "
                    f"ожидание стабилизации {settle_left} сек. "
                    f"перед командой {operation}"
                )

                _ui_message(
                    db,
                    msg,
                    Severity.warn,
                    message_key="equipment.im",
                )
                repo.set_setting(db, "im_connected", True)
                repo.set_setting(db, "im_error", "")

                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "where": "IM_RECONNECT_SETTLE",
                        "cmd": operation,
                        "remaining_sec": int(settle_left),
                    },
                )

                return False, msg

            repo.set_setting(db, "im_connected", True)
            repo.set_setting(db, "im_error", "")

            if not was_connected:
                # После reconnect не доверяем старой программе:
                # ИМ могла её сбросить.
                repo.set_setting(db, "im_loaded_program", "")
                repo.add_event(
                    db,
                    severity=Severity.warn.value,
                    source="DAEMON",
                    type_=EventType.STATE_UPDATED.value,
                    payload={
                        "message": "IM reconnected; loaded program invalidated",
                        "cmd": operation,
                    },
                )

            return True, None

        if cmd_type == CommandType.IM_CLEAR_DB.value:
            batch_id = (payload or {}).get("batch_id")
            operation_id = (payload or {}).get("operation_id")
            phase = (payload or {}).get("phase")

            # Не выполняем устаревшую команду от уже завершённой
            # или сменившейся партии.
            if not _guard_current_operation(
                db,
                where=f"IM_CLEAR_DB:{phase or '-'}:start",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return True, None

            ok_conn, err_conn = await _ensure_im_connected("clear_db")
            if not ok_conn:
                return False, err_conn

            if not im.nodes.clear_db:
                return False, "IM_NODE_CLEAR_DB is not configured"

            im.request_clear_db()

            # Такой же bool-импульс, как у measure_start:
            # первый poll записывает True, второй — False.
            await im.poll_once()
            await asyncio.sleep(0.05)
            await im.poll_once()

            if not _connected_attr(im, default=False):
                err = _last_error_attr(im) or "not connected"
                return False, (
                    "Микрометр: связь потеряна при очистке журнала измерений "
                    f"({err})"
                )

            if not _guard_current_operation(
                db,
                where=f"IM_CLEAR_DB:{phase or '-'}:done",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return True, None

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.STATE_UPDATED.value,
                payload={
                    "where": "IM_CLEAR_DB",
                    "message": "IM measurement journal cleared",
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                    "phase": phase,
                },
            )

            return True, None

        if cmd_type == CommandType.IM_VACUUM_ON.value:
            ok_conn, err_conn = await _ensure_im_connected("vacuum_on")
            if not ok_conn:
                return False, err_conn

            im.request_vacuum(True)
            await im.poll_once()

            if not _connected_attr(im, default=False):
                return False, "Микрометр: связь потеряна при включении вакуума"

            repo.add_event(
                db,
                severity="info",
                source="DAEMON",
                type_=EventType.IM_VACUUM_SET.value,
                payload={"on": True},
            )
            return True, None

        if cmd_type == CommandType.IM_VACUUM_OFF.value:
            ok_conn, err_conn = await _ensure_im_connected("vacuum_off")
            if not ok_conn:
                return False, err_conn

            im.request_vacuum(False)
            await im.poll_once()

            if not _connected_attr(im, default=False):
                return False, "Микрометр: связь потеряна при выключении вакуума"

            repo.add_event(
                db,
                severity="info",
                source="DAEMON",
                type_=EventType.IM_VACUUM_SET.value,
                payload={"on": False},
            )
            return True, None

        if cmd_type == CommandType.IM_LOAD_PROGRAM.value:
            pc = (payload or {}).get("product_code")
            if not pc:
                return False, "payload.product_code is required"

            batch_id = (payload or {}).get("batch_id")
            operation_id = (payload or {}).get("operation_id")
            after_calibration = bool((payload or {}).get("after_calibration"))
            phase = (payload or {}).get("phase")

            delay_before_load_sec = float(
                (payload or {}).get("delay_before_load_sec") or 0.0
            )
            settle_after_load_sec = float(
                (payload or {}).get("settle_after_load_sec") or 0.0
            )

            next_calib_mode = (payload or {}).get("next_calib_mode")

            ok_conn, err_conn = await _ensure_im_connected("load_program")
            if not ok_conn:
                return False, err_conn

            return await _load_im_program_until_ok(
                db,
                str(pc),
                batch_id=batch_id,
                operation_id=operation_id,
                after_calibration=after_calibration,
                phase=phase,
                delay_before_load_sec=delay_before_load_sec,
                settle_after_load_sec=settle_after_load_sec,
                next_calib_mode=next_calib_mode,
            )

        if cmd_type == CommandType.IM_CALIBRATE.value:
            ok_conn, err_conn = await _ensure_im_connected("im_calibrate")
            if not ok_conn:
                return False, err_conn

            t = im_tasks.get("calibrate")
            if t is not None and not t.done():
                # Идемпотентная защита от уже поставленной команды.
                # Повтор не является ошибкой и не должен сбрасывать latch.
                return True, "skipped: calibration already running"

            mode = int((payload or {}).get("pulse", 0))
            if mode not in (1, 2):
                return False, "payload.pulse must be 1 or 2"

            batch_id = (payload or {}).get("batch_id")
            operation_id = (payload or {}).get("operation_id")

            if not _guard_current_operation(
                db,
                where="IM_CALIBRATE_COMMAND",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return True, None

            op_name = "проверка" if mode == 2 else "калибровка"

            required_program = str(
                (payload or {}).get("required_program") or ""
            ).strip()
            if required_program:
                loaded_im_program = str(
                    repo.get_setting(db, "im_loaded_program", "") or ""
                ).strip()

                if loaded_im_program != required_program:
                    ok_load, err_load = await _load_im_program_until_ok(
                        db,
                        required_program,
                        batch_id=(payload or {}).get("batch_id"),
                        operation_id=operation_id,
                        phase="pre_calibration_required",
                        settle_after_load_sec=float(
                            (payload or {}).get(
                                "required_program_settle_after_load_sec"
                            )
                            or 6.0
                        ),
                        next_calib_mode=mode,
                    )
                    if not ok_load:
                        return False, err_load

            # Новая калибровка/проверка фактически начинается.
            # Инструкция по предыдущему неуспешному результату (-1/-3)
            # больше не актуальна и снимается до публикации текущего workflow.
            quality_message_key = _batch_operator_message_key(
                QUALITY_CALIBRATION_MESSAGE_PREFIX,
                batch_id,
            )
            if quality_message_key:
                repo.clear_operator_message(
                    db,
                    quality_message_key,
                )

            _ui_message(
                db,
                f"Микрометр: {op_name} запущена",
                Severity.info,
                message_key=(
                    _batch_operator_message_key(
                        IM_WORKFLOW_MESSAGE_PREFIX,
                        batch_id,
                    )
                    or IM_WORKFLOW_MESSAGE_PREFIX
                ),
            )

            repo.add_event(
                db,
                severity=Severity.info.value,
                source="DAEMON",
                type_=EventType.IM_CALIBRATION_STARTED.value,
                payload={
                    "mode": mode,
                    "operation": op_name,
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                    "reason": (payload or {}).get("reason"),
                },
            )
            db.commit()

            im_tasks["calibrate"] = asyncio.create_task(
                _start_im_calibration(
                    mode=mode,
                    batch_id=batch_id,
                    operation_id=operation_id,
                )
            )

            return True, None

        if cmd_type == CommandType.IM_MEASURE_ONCE.value:
            ok_conn, err_conn = await _ensure_im_connected("im_measure_once")
            if not ok_conn:
                return False, err_conn

            pc = (payload or {}).get("product_code")
            if not pc:
                return False, "payload.product_code is required"

            batch_id = (payload or {}).get("batch_id")
            operation_id = (payload or {}).get("operation_id")

            if not _guard_current_operation(
                db,
                where="IM_MEASURE_ONCE_COMMAND",
                batch_id=batch_id,
                operation_id=operation_id,
            ):
                return True, None

            t = im_tasks.get("measure")
            if t is not None and not t.done():
                return True, "skipped: measurement already running"

            _ui_message(
                db,
                f"Микрометр: измерение детали {pc} запущено",
                Severity.info,
                message_key=(
                    _batch_operator_message_key(
                        IM_WORKFLOW_MESSAGE_PREFIX,
                        batch_id,
                    )
                    or IM_WORKFLOW_MESSAGE_PREFIX
                ),
            )

            repo.add_event(
                db,
                severity="warn",
                source="DAEMON",
                type_=EventType.IM_MEASUREMENT_STARTED.value,
                payload={
                    "product_code": pc,
                    "batch_id": batch_id,
                    "operation_id": operation_id,
                },
            )

            im_tasks["measure"] = asyncio.create_task(
                _start_im_measure_once(
                    str(pc),
                    batch_id=batch_id,
                    operation_id=operation_id,
                )
            )

            return True, None

        return False, f"unknown IM command: {cmd_type}"

    # main loop
    last_operator_message_cleanup_at = 0.0

    try:
        while True:
            # cleanup finished IM tasks (log exceptions)
            for k, tsk in list(im_tasks.items()):
                if tsk.done():
                    exc = tsk.exception()
                    if exc:
                        dbx = SessionLocal()
                        try:
                            repo.add_event(
                                dbx,
                                severity="error",
                                source="DAEMON",
                                type_=EventType.ERROR.value,
                                payload={"where": f"im_task:{k}", "err": str(exc)},
                            )
                        finally:
                            dbx.close()
                    im_tasks.pop(k, None)

            # 1) refresh IO snapshot
            if hasattr(io, "poll_once"):
                try:
                    await io.poll_once()
                except Exception:
                    pass

            # IM snapshot (второй OPCUA)
            if im is not None:
                try:
                    await im.poll_once()
                except Exception:
                    pass

            # 2) refresh RTK snapshot + flush queued commands
            try:
                if hasattr(rtk, "poll_once"):
                    await _maybe_await(rtk.poll_once())
            except Exception as e:
                db = SessionLocal()
                try:
                    repo.add_event(
                        db,
                        severity="error",
                        source="DAEMON",
                        type_=EventType.ERROR.value,
                        payload={"where": "RTK_POLL", "err": str(e)},
                    )
                    try:
                        _set_message(
                            db,
                            f"РТК: ошибка обмена ({e})",
                            Severity.error,
                            message_key="equipment.rtk",
                            rtk_connected=0,
                            rtk_error=str(e),
                        )
                    except Exception:
                        pass
                finally:
                    db.close()

            # 3) process commands + tick supervisor
            db = SessionLocal()
            try:
                # TTL-сообщения очищаются независимо от активности API/UI,
                # но не чаще одного раза в секунду, чтобы не нагружать SQLite.
                cleanup_now = time.monotonic()
                if (
                    cleanup_now - last_operator_message_cleanup_at
                    >= OPERATOR_MESSAGE_CLEANUP_PERIOD_SEC
                ):
                    repo.cleanup_expired_operator_messages(db)
                    last_operator_message_cleanup_at = cleanup_now

                await _publish_equipment_status(db)

                cmd = repo.claim_next_command(db)
                if cmd:
                    ok = False
                    err = None

                    try:
                        if cmd.type in im_command_types:
                            ok, err = await handle_im_command(
                                db, cmd.type, cmd.payload or {}
                            )
                        else:
                            ok, err = sup.handle_command(
                                db,
                                cmd.type,
                                cmd.payload or {},
                                created_by=cmd.created_by,
                            )

                    except Exception as e:
                        ok = False
                        err = _short_err(e)
                        repo.add_event(
                            db,
                            severity=Severity.error.value,
                            source="DAEMON",
                            type_=EventType.ERROR.value,
                            payload={
                                "where": "COMMAND_DISPATCH",
                                "command_id": cmd.id,
                                "type": cmd.type,
                                "err": err,
                            },
                        )

                    repo.finish_command(db, cmd.id, ok=ok, error=err)

                    if not ok and err:
                        rejectbin_workflow_failure = (
                            _rejectbin_command_uses_workflow_message(
                                db,
                                cmd_type=cmd.type,
                            )
                        )

                        # Потеря связи с IM уже отображается отдельным
                        # аппаратным ключом equipment.im. Не создаём второй
                        # sticky command.error с тем же текстом: после
                        # восстановления equipment.im снимается сразу, тогда
                        # как command.error ожидал бы следующей успешной
                        # команды того же типа и оставался устаревшим.
                        im_equipment_failure = (
                            _im_failure_has_dedicated_equipment_message(cmd.type)
                        )
                        modal_only_failure = _command_uses_modal_only_feedback(
                            cmd.type,
                            cmd.payload or {},
                        )

                        if modal_only_failure:
                            try:
                                operator_error = _operator_command_error_message(
                                    cmd.type,
                                    err,
                                )
                                _clear_modal_feedback_duplicate(
                                    db,
                                    error_text=str(err),
                                    operator_text=operator_error,
                                )
                            except Exception as message_exc:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass

                                try:
                                    repo.add_event(
                                        db,
                                        severity=Severity.error.value,
                                        source="DAEMON",
                                        type_=EventType.ERROR.value,
                                        payload={
                                            "where": "MODAL_COMMAND_MESSAGE_CLEAR",
                                            "command_id": cmd.id,
                                            "type": cmd.type,
                                            "err": _short_err(message_exc),
                                        },
                                    )
                                except Exception:
                                    try:
                                        db.rollback()
                                    except Exception:
                                        pass

                        elif rejectbin_workflow_failure:
                            try:
                                _clear_duplicate_command_error(
                                    db,
                                    error_text=str(err),
                                )
                            except Exception as message_exc:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass

                                try:
                                    repo.add_event(
                                        db,
                                        severity=Severity.error.value,
                                        source="DAEMON",
                                        type_=EventType.ERROR.value,
                                        payload={
                                            "where": (
                                                "REJECTBIN_COMMAND_ERROR_"
                                                "DUPLICATE_CLEAR"
                                            ),
                                            "command_id": cmd.id,
                                            "type": cmd.type,
                                            "err": _short_err(message_exc),
                                        },
                                    )
                                except Exception:
                                    try:
                                        db.rollback()
                                    except Exception:
                                        pass

                        elif im_equipment_failure:
                            try:
                                _clear_duplicate_command_error(
                                    db,
                                    error_text=str(err),
                                )
                            except Exception as message_exc:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass

                                try:
                                    repo.add_event(
                                        db,
                                        severity=Severity.error.value,
                                        source="DAEMON",
                                        type_=EventType.ERROR.value,
                                        payload={
                                            "where": ("COMMAND_ERROR_DUPLICATE_CLEAR"),
                                            "command_id": cmd.id,
                                            "type": cmd.type,
                                            "err": _short_err(message_exc),
                                        },
                                    )
                                except Exception:
                                    try:
                                        db.rollback()
                                    except Exception:
                                        pass

                        # Остальные ошибки команд по-прежнему публикуются
                        # через command.error и снимаются только после
                        # успешного повтора команды того же типа.
                        else:
                            try:
                                _set_message(
                                    db,
                                    _operator_command_error_message(
                                        cmd.type,
                                        err,
                                    ),
                                    Severity.error,
                                    message_key="command.error",
                                )
                            except Exception as message_exc:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass

                                try:
                                    repo.add_event(
                                        db,
                                        severity=Severity.error.value,
                                        source="DAEMON",
                                        type_=EventType.ERROR.value,
                                        payload={
                                            "where": "COMMAND_ERROR_MESSAGE_PUBLISH",
                                            "command_id": cmd.id,
                                            "type": cmd.type,
                                            "err": _short_err(message_exc),
                                        },
                                    )
                                except Exception:
                                    try:
                                        db.rollback()
                                    except Exception:
                                        pass
                    elif ok:
                        # Команды модалки извлечения не владеют общим
                        # command.error и не должны снимать чужую ошибку.
                        if _command_uses_modal_only_feedback(
                            cmd.type,
                            cmd.payload or {},
                        ):
                            pass
                        # command.error описывает последнюю неуспешную
                        # команду. Убираем его только после успешного
                        # выполнения повторной команды того же типа.
                        else:
                            try:
                                _clear_resolved_command_error(
                                    db,
                                    successful_command=cmd,
                                )
                            except Exception as message_exc:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass

                                try:
                                    repo.add_event(
                                        db,
                                        severity=Severity.error.value,
                                        source="DAEMON",
                                        type_=EventType.ERROR.value,
                                        payload={
                                            "where": "COMMAND_ERROR_MESSAGE_CLEAR",
                                            "command_id": cmd.id,
                                            "type": cmd.type,
                                            "err": _short_err(message_exc),
                                        },
                                    )
                                except Exception:
                                    try:
                                        db.rollback()
                                    except Exception:
                                        pass

                    if not ok:
                        # Сбрасываем только latch операции,
                        # которая действительно завершилась ошибкой.
                        try:
                            if cmd.type == CommandType.IM_MEASURE_ONCE.value:
                                sup._mm_req_sent_batch = None

                            elif cmd.type == CommandType.IM_LOAD_PROGRAM.value:
                                sup._calib_program_load_req_key = None

                            elif cmd.type == CommandType.IM_CALIBRATE.value:
                                sup._calib_req_sent_batch = None

                                repo.set_state(
                                    db,
                                    calibration_inflight=0,
                                    calib_result_inflight=0,
                                )

                        except Exception:
                            pass

                    repo.add_event(
                        db,
                        severity=("info" if ok else "error"),
                        source="DAEMON",
                        type_=("COMMAND_DONE" if ok else "COMMAND_FAILED"),
                        payload={
                            "command_id": cmd.id,
                            "type": cmd.type,
                            "error": err,
                            "created_by": cmd.created_by,
                        },
                    )

                sup.tick(db)

            finally:
                db.close()

            await asyncio.sleep(0.2)

    finally:
        if hasattr(io, "disconnect"):
            try:
                await io.disconnect()
            except Exception:
                pass

        if im is not None:
            try:
                await im.disconnect()
            except Exception:
                pass

        if hasattr(rtk, "disconnect"):
            try:
                await _maybe_await(rtk.disconnect())
            except Exception:
                pass


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
