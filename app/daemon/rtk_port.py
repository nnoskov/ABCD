from dataclasses import dataclass
from typing import Optional, Protocol

@dataclass(frozen=True)
class RTKSnapshot:
    connected: bool
    state: str
    busy: bool
    error: Optional[str] = None

    action_r1: str | None = None
    action_r2: str | None = None

    cs_r1: bool | None = None
    cs_r2: bool | None = None

    # Диагностика условий включения cycle_on.
    # r1 = RS013N, r2 = RS007L.
    teach_r1: bool | None = None
    teach_r2: bool | None = None
    teachl_r1: bool | None = None
    teachl_r2: bool | None = None
    tpemg_r1: bool | None = None
    tpemg_r2: bool | None = None
    opemg_r1: bool | None = None
    opemg_r2: bool | None = None
    exemg_r1: bool | None = None
    exemg_r2: bool | None = None
    robot_error_r1: bool | None = None
    robot_error_r2: bool | None = None
    ecode_r1: int | None = None
    ecode_r2: int | None = None

    watchdog_r1: bool | None = None
    watchdog_r2: bool | None = None
        
    connected_r1: bool | None = None
    connected_r2: bool | None = None

    pickcount: Optional[int] = None
    defectcount: Optional[int] = None
    robot_state: Optional[int] = None

    tarein: int | None = None   # r1 (rs013n)
    tareout: int | None = None   # r1 (rs013n)
    putcount: int | None = None  # r2 (rs007l)
    
    air_pressure_ok: Optional[bool] = True
    positioner_part_present: Optional[bool] = None    
    
class RTKPort(Protocol):
    def request(self, cmd: str, payload: Optional[dict] = None) -> None: ...
    def snapshot(self) -> RTKSnapshot: ...
    async def poll_once(self) -> None: ...