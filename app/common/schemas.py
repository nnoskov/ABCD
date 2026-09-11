from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any, Dict, Optional
from .enums import CommandType, CommandStatus, EventType, Severity, SystemMode

class CommandCreate(BaseModel):
    type: CommandType
    payload: Dict[str, Any] = {}

class CommandOut(BaseModel):
    id: int
    created_at: datetime
    created_by: Optional[str] = None
    type: CommandType
    status: CommandStatus
    payload: Dict[str, Any]
    error: Optional[str] = None

class EventOut(BaseModel):
    id: int
    ts: datetime
    severity: Severity
    source: str
    type: str   #EventType
    payload: Dict[str, Any]

class OperatorMessage(BaseModel):
    key: str
    message: str
    severity: Severity
    priority: int
    sticky: bool = True
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime] = None


class SystemState(BaseModel):
    updated_at: datetime
    mode: SystemMode
    
    safety_ok: bool = True
    reject_bin_count: int = 0
    reject_bin_capacity: int = 0
    active_batch_id: Optional[int] = None
    active_operation_id: Optional[str] = None
    active_operation_batch_id: Optional[int] = None
    active_operation_phase: Optional[str] = None
    active_operation_started_at: Optional[datetime] = None    
    message: Optional[str] = None
    message_severity: str = "info"
    operator_messages: list[OperatorMessage] = Field(default_factory=list)
    trash_present: bool = True
    air_pressure_ok: bool = True
    
    rtk_connected: bool = False
    rtk_busy: bool = False
    rtk_state: str = "unknown"
    rtk_action_r1: str = "unknown"
    rtk_action_r2: str = "unknown"
    rtk_error: str | None = None
    rtk_manual_mode: bool = False
    need_cycle_on: bool = False
    
    pending_mm_result: bool | None = None
    mm_result_inflight: bool = False
    
    pending_calib_result: int | None = None
    calib_result_inflight: bool = False

    # measurement / rules
    consecutive_rejects: int = 0

    last_meas_ok: bool | None = None
    last_meas_not_ok: int | None = None
    last_meas_summary: dict[str, Any] | None = None  # json-строка

    calibration_inflight: bool = False
    calibration_reason: str | None = None
