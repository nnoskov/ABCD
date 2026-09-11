from sqlalchemy import Column, DateTime, Integer, String, Text, Boolean, ForeignKey, CheckConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import JSON

from app.common.enums import CommandStatus, Severity, SystemMode, UserRole
from app.common.timeutils import utcnow

Base = declarative_base()


class UserRow(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "role IN ('operator', 'admin')",
            name="ck_users_role",
        ),
    )

    id = Column(Integer, primary_key=True)
    login = Column(String(128), unique=True, index=True, nullable=False)
    display_name = Column(String(128), nullable=False)
    role = Column(
        String(16),
        default=UserRole.operator.value,
        index=True,
        nullable=False,
    )
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, index=True, nullable=False)
    created_at = Column(DateTime, default=utcnow, index=True, nullable=False)


class UserSessionRow(Base):
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        index=True,
        nullable=False,
    )
    token_hash = Column(String(128), unique=True, index=True, nullable=False)
    created_at = Column(DateTime, default=utcnow, index=True, nullable=False)
    expires_at = Column(DateTime, index=True, nullable=False)


class CommandRow(Base):
    __tablename__ = "commands"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=utcnow, index=True)
    created_by = Column(String(128), nullable=True)

    type = Column(String(64), index=True, nullable=False)
    status = Column(String(16), default=CommandStatus.pending.value, index=True, nullable=False)

    payload = Column(JSON, default=dict, nullable=False)
    error = Column(Text, nullable=True)


class EventRow(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=utcnow, index=True)
    severity = Column(String(16), default=Severity.info.value, index=True, nullable=False)
    source = Column(String(32), default="CORE", index=True, nullable=False)
    type = Column(String(64), index=True, nullable=False)
    payload = Column(JSON, default=dict, nullable=False)


class SystemStateRow(Base):
    __tablename__ = "system_state"

    id = Column(Integer, primary_key=True)  # всегда 1
    updated_at = Column(DateTime, default=utcnow)

    mode = Column(String(32), default=SystemMode.idle.value, nullable=False)
    
    safety_ok = Column(Integer, default=1, nullable=False)
    reject_bin_count = Column(Integer, default=0, nullable=False)
    reject_bin_capacity = Column(Integer, default=0, nullable=False)
    active_batch_id = Column(Integer, nullable=True)
    active_batch_phase = Column(String(16), nullable=True)  # starting|running
    active_batch_expected_count = Column(Integer, nullable=True)    # ProductCount
    active_operation_id = Column(String(64), nullable=True)
    active_operation_batch_id = Column(Integer, nullable=True)
    active_operation_phase = Column(String(32), nullable=True)  # starting|running|finishing
    active_operation_started_at = Column(DateTime, nullable=True)    

    message = Column(Text, nullable=True)
    trash_present = Column(Integer, default=1, nullable=False)
    
    rtk_connected = Column(Integer, default=0, nullable=False)
    rtk_busy = Column(Integer, default=0, nullable=False)
    rtk_state = Column(String(64), default="unknown", nullable=False)
    rtk_action_r1 = Column(String(64), default="unknown", nullable=False)
    rtk_action_r2 = Column(String(64), default="unknown", nullable=False)
    rtk_error = Column(Text, nullable=True)
    rtk_pickcount = Column(Integer, nullable=True)
    rtk_defectcount = Column(Integer, nullable=True)
    rtk_pickcount_seen = Column(Integer, nullable=True)
    rtk_defectcount_seen = Column(Integer, nullable=True)
    rtk_consecutive_defects = Column(Integer, default=0, nullable=False)
    rtk_paused = Column(Integer, default=0, nullable=False)
    rtk_pause_reason = Column(String(64), nullable=True)
    
    pending_mm_result = Column(Integer, nullable=True)          # 1/0
    mm_result_inflight = Column(Integer, default=0, nullable=False)
    
    pending_calib_result = Column(Integer, nullable=True)       # 0/-1/-2
    calib_result_inflight = Column(Integer, default=0, nullable=False)

    consecutive_rejects = Column(Integer, default=0, nullable=False)

    last_meas_ok = Column(Integer, nullable=True)       # 1/0/NULL
    last_meas_not_ok = Column(Integer, nullable=True)   # сколько NOK-размеров
    last_meas_summary = Column(Text, nullable=True)     # json-строка (короткая сводка)

    calibration_inflight = Column(Integer, default=0, nullable=False)   # 1/0
    calibration_reason = Column(Text, nullable=True)

    # если нужно переключить ИМ на программу с исполнением после OK-калибровки
    calib_wait_im_program = Column(Integer, default=0, nullable=False)   # 1/0
    calib_im_program_target = Column(Text, nullable=True)               # "123.242.436-01"

class BatchRow(Base):
    __tablename__ = "batches"

    id = Column(Integer, primary_key=True)

    status = Column(String(32), index=True, nullable=False, default="loaded")  # loaded|auto_processing|done|rejected|extracted
    created_at = Column(DateTime, default=utcnow, index=True)
    updated_at = Column(DateTime, default=utcnow)

    loaded_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    extracted_at = Column(DateTime, nullable=True)

    reject_reason = Column(Text, nullable=True)

    # вся “паспортная” инфа партии + доп. поля
    data = Column(JSON, default=dict, nullable=False)

    # счётчики измерений (ведём отдельно от data, но дублируем в data для UI/экспорта)
    measured_good = Column(Integer, default=0, nullable=False)
    measured_bad = Column(Integer, default=0, nullable=False)

    # где лежит партия (шкаф/ячейка/колонка) - пока в JSON, чтобы не тормозить
    location = Column(JSON, default=dict, nullable=False)


class BatchMeasurementRow(Base):
    __tablename__ = "batch_measurements"

    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=utcnow, index=True)

    batch_id = Column(Integer, index=True, nullable=True)

    part_ok = Column(Integer, default=1, nullable=False)     # 1/0
    not_ok = Column(Integer, default=0, nullable=False)      # сколько NOK-размеров
    counted = Column(Integer, nullable=True)                 # 1/0/NULL (повлияло ли на счётчики партии)

    values = Column(JSON, default=dict, nullable=False)      # {name: value}
    tols = Column(JSON, default=dict, nullable=False)        # {name: bool}
    items = Column(JSON, default=list, nullable=False)       # [{name,value,ok}, ...]


class RejectBinRow(Base):
    __tablename__ = "reject_bins"

    id = Column(Integer, primary_key=True)
    opened_at = Column(DateTime, default=utcnow, index=True)
    closed_at = Column(DateTime, nullable=True)

    tare_no = Column(Integer, nullable=True, index=True)    # Физический номер тары брака: 1..10

    is_active = Column(Boolean, default=True, index=True, nullable=False)
    capacity = Column(Integer, default=0, nullable=False)
    count_at_close = Column(Integer, default=0, nullable=False)
    close_reason = Column(String(64), nullable=True)


class RejectBinItemRow(Base):
    __tablename__ = "reject_bin_items"

    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=utcnow, index=True)

    reject_bin_id = Column(Integer, index=True, nullable=False)
    batch_id = Column(Integer, nullable=True)
    measured_params = Column(JSON, default=dict, nullable=False)
    reason = Column(String(256), nullable=True)


class SettingRow(Base):
    __tablename__ = "settings"

    key = Column(String(64), primary_key=True)
    updated_at = Column(DateTime, default=utcnow)
    value = Column(JSON, nullable=False)
