from __future__ import annotations

import time
import math

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from app.daemon.asyncua_service import AsyncuaService

def _unwrap_opcua(v: Any) -> Any:
    # asyncua часто отдаёт DataValue/Variant, их надо развернуть в python-значение
    if v is None:
        return None

    # DataValue.Value -> Variant, Variant.Value -> python
    try:
        if hasattr(v, "Value"):
            vv = getattr(v, "Value")
            if hasattr(vv, "Value"):
                return getattr(vv, "Value")
            if hasattr(vv, "value"):
                return getattr(vv, "value")
            return vv
    except Exception:
        pass

    # иногда бывает .value
    try:
        if hasattr(v, "value"):
            return getattr(v, "value")
    except Exception:
        pass

    return v

def _as_bool(v: Any, default: bool = False) -> bool:
    v = _unwrap_opcua(v)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "t", "yes", "y", "on", "True"):
            return True
        if s in ("0", "false", "f", "no", "n", "off", "False"):
            return False
    return default


def _as_int(v: Any, default: int = 0) -> int:
    v = _unwrap_opcua(v)
    try:
        return int(v)
    except Exception:
        return default


def _as_float(v: Any, default: float | None = None) -> float | None:
    v = _unwrap_opcua(v)
    if v is None:
        return default
    try:
        f = float(v)
        if not math.isfinite(f):
            return default
        return f
    except Exception:
        return default


@dataclass(frozen=True)
class OpcUaNodes:
    # inputs
    safety_ok: str
    trashcan_present: str
    safety_status: Optional[str] = None

    # optional inputs (temperature sensors {float})
    temperature_sensor_loading: Optional[str] = None  # T_1
    temperature_sensor_im: Optional[str] = None       # T_2

    # optional inputs (statuses/sensors)
    loading_door_stat_fmt: Optional[str] = None         # e.g. "StatDL{idx}"
    unloading_door_stat_fmt: Optional[str] = None       # e.g. "StatDU{idx}"
    loading_col_sensor_fmt: Optional[str] = None        # e.g. "OSL_{col}"
    unloading_col_sensor_fmt: Optional[str] = None      # e.g. "OSU_{col}"

    # outputs
    loading_door_cmd_fmt: Optional[str] = None          # e.g. "LDoorCmd{idx}"
    unloading_door_cmd_fmt: Optional[str] = None        # e.g. "UDoorCmd{idx}"

    # stacklight
    stacklight_color: Optional[str] = None
    stacklight_sound: Optional[str] = None



class OpcUaIO:
    """High-level Postamat IO on top of AsyncuaService.
    Reads:
      - subscription-driven snapshot + periodic fallback read

    Writes:
      - dedup writes for stacklight
      - 2-phase pulses for door commands (ON -> next poll OFF)
    """

    def __init__(
        self,
        *,
        endpoint: str,
        nodes: OpcUaNodes,
        svc: AsyncuaService,
        read_fallback_sec: float = 2.0,
        now: Callable[[], float] = time.monotonic,
        loading_cells: Optional[Iterable[int]] = None,
        unloading_cells: Optional[Iterable[int]] = None,
        loading_cols: Optional[Iterable[int]] = None,
        unloading_cols: Optional[Iterable[int]] = None,
    ):
        self.endpoint = endpoint
        self.nodes = nodes
        self.svc = svc

        self.read_fallback_sec = float(read_fallback_sec)
        self._now = now

        self._loading_cells = list(loading_cells or [])
        self._unloading_cells = list(unloading_cells or [])
        self._loading_cols = list(loading_cols or [])
        self._unloading_cols = list(unloading_cols or [])

        # snapshot of last known inputs (subscription + fallback reads)
        self._snap: dict[str, Any] = {}

        # last fallback read timestamp
        self._last_fallback = 0.0

        # outputs desired state
        self._desired_stack_color_int: Optional[int] = None
        self._desired_stack_sound_int: Optional[int] = None        
        
        self._last_written: dict[str, Any] = {}
        self._write_vtypes: dict[str, Any] = {}

        # pulses: node_id -> stage (0: need ON, 1: need OFF next poll)
        self._pulses: dict[str, int] = {}

        self._last_error: Optional[str] = None

    # -----------------
    # lifecycle
    # -----------------
    @property
    def connected(self) -> bool:
        return bool(getattr(self.svc, "connected", False))
    
    @property
    def last_error(self) -> str | None:
        return self._last_error or getattr(self.svc, "last_error", None)
    
    def _mark_disconnected(self, err: Any = None) -> None:
        if err:
            self._last_error = str(err)
    
        # fail-safe: при потере связи аварийный контур недоступен
        self._snap[self.nodes.safety_ok] = False
        self._snap[self.nodes.trashcan_present] = False
    
        if self.nodes.safety_status:
            self._snap.pop(self.nodes.safety_status, None)


    async def connect(self) -> None:
        try:
            await self.svc.connect()
            await self._subscribe_inputs()
            self._last_error = None
        except Exception as e:
            self._mark_disconnected(repr(e))
            raise
    
    
    async def disconnect(self) -> None:
        self._mark_disconnected("disconnected")
        await self.svc.disconnect()
    
    
    async def poll_once(self) -> None:
        was_connected = self.connected
    
        try:
            connected = await self.svc.ensure_connected()
        except Exception as e:
            self._mark_disconnected(repr(e))
            return
    
        if not connected:
            self._mark_disconnected(getattr(self.svc, "last_error", None) or "not connected")
            return
    
        if not was_connected:
            try:
                await self._subscribe_inputs()
            except Exception as e:
                self._mark_disconnected(repr(e))
                await self.svc.disconnect()
                return
    
            self._last_written.clear()
            self._last_error = None
    
        try:
            await self._flush_pulses()
            await self._flush_stacklight()
    
            if self.read_fallback_sec <= 0:
                await self._fallback_read()
            else:
                now = self._now()
                if (now - self._last_fallback) >= self.read_fallback_sec:
                    await self._fallback_read()
    
        except Exception as e:
            self._mark_disconnected(repr(e))
            await self.svc.disconnect()

    # -----------------
    # subscription snapshot
    # -----------------
    async def _subscribe_inputs(self) -> None:
        # mandatory inputs
        await self._subscribe_one(self.nodes.safety_ok)
        await self._subscribe_one(self.nodes.trashcan_present)

        if self.nodes.safety_status:
            await self._subscribe_one(self.nodes.safety_status)

        await self._subscribe_one(self.nodes.temperature_sensor_loading)
        await self._subscribe_one(self.nodes.temperature_sensor_im)

        # optional statuses/sensors if we know the lists
        if self.nodes.loading_door_stat_fmt:
            for idx in self._loading_cells:
                await self._subscribe_one(self.nodes.loading_door_stat_fmt.format(idx=int(idx)))
        if self.nodes.unloading_door_stat_fmt:
            for idx in self._unloading_cells:
                await self._subscribe_one(self.nodes.unloading_door_stat_fmt.format(idx=int(idx)))
        if self.nodes.loading_col_sensor_fmt:
            for col in self._loading_cols:
                await self._subscribe_one(self.nodes.loading_col_sensor_fmt.format(col=int(col)))
        if self.nodes.unloading_col_sensor_fmt:
            for col in self._unloading_cols:
                await self._subscribe_one(self.nodes.unloading_col_sensor_fmt.format(col=int(col)))


    async def _subscribe_one(self, node_id: str | None) -> None:
        if not node_id:
            return
    
        nid = str(node_id)
    
        def cb(*args, _nid=nid):
            if len(args) == 1:
                self._on_datachange(_nid, args[0])
            else:
                self._on_datachange(str(args[0]), args[1])
    
        await self.svc.subscribe_data_change(nid, cb)


    def _on_datachange(self, node_id: str, value: Any) -> None:
        self._snap[str(node_id)] = _unwrap_opcua(value)


    async def _fallback_read(self) -> None:
        self._last_fallback = self._now()
    
        self._snap[self.nodes.safety_ok] = _unwrap_opcua(
            await self.svc.read_value(self.nodes.safety_ok, default=False)
        )
        if not self.connected:
            self._mark_disconnected(getattr(self.svc, "last_error", None) or "not connected")
            return
    
        self._snap[self.nodes.trashcan_present] = _unwrap_opcua(
            await self.svc.read_value(self.nodes.trashcan_present, default=False)
        )
        if not self.connected:
            self._mark_disconnected(getattr(self.svc, "last_error", None) or "not connected")
            return
    
        if self.nodes.safety_status:
            self._snap[self.nodes.safety_status] = _unwrap_opcua(
                await self.svc.read_value(self.nodes.safety_status, default=None)
            )
    
        if self.nodes.temperature_sensor_loading:
            self._snap[str(self.nodes.temperature_sensor_loading)] = _unwrap_opcua(
                await self.svc.read_value(self.nodes.temperature_sensor_loading, default=None)
            )
    
        if self.nodes.temperature_sensor_im:
            self._snap[str(self.nodes.temperature_sensor_im)] = _unwrap_opcua(
                await self.svc.read_value(self.nodes.temperature_sensor_im, default=None)
            )
    
        if self.nodes.loading_door_stat_fmt:
            for idx in self._loading_cells:
                nid = self.nodes.loading_door_stat_fmt.format(idx=int(idx))
                self._snap[nid] = _unwrap_opcua(
                    await self.svc.read_value(nid, default=self._snap.get(nid))
                )
    
        if self.nodes.unloading_door_stat_fmt:
            for idx in self._unloading_cells:
                nid = self.nodes.unloading_door_stat_fmt.format(idx=int(idx))
                self._snap[nid] = _unwrap_opcua(
                    await self.svc.read_value(nid, default=self._snap.get(nid))
                )
    
        if self.nodes.loading_col_sensor_fmt:
            for col in self._loading_cols:
                nid = self.nodes.loading_col_sensor_fmt.format(col=int(col))
                self._snap[nid] = _unwrap_opcua(
                    await self.svc.read_value(nid, default=self._snap.get(nid))
                )
    
        if self.nodes.unloading_col_sensor_fmt:
            for col in self._unloading_cols:
                nid = self.nodes.unloading_col_sensor_fmt.format(col=int(col))
                self._snap[nid] = _unwrap_opcua(
                    await self.svc.read_value(nid, default=self._snap.get(nid))
                )

    # -----------------
    # reads
    # -----------------
    def read_safety_ok(self) -> bool:
        return _as_bool(self._snap.get(self.nodes.safety_ok), default=False)

    def read_safety_status(self) -> int | None:
        if not self.nodes.safety_status:
            return None

        raw = _unwrap_opcua(self._snap.get(self.nodes.safety_status))
        if raw is None:
            return None

        try:
            return int(raw)
        except Exception:
            return None
            

    def read_trashcan_present(self) -> bool:
        return _as_bool(self._snap.get(self.nodes.trashcan_present), default=False)

    def read_temperature_sensor_loading(self) -> float | None:
        if not self.nodes.temperature_sensor_loading:
            return None
        return _as_float(self._snap.get(str(self.nodes.temperature_sensor_loading)), default=None)

    def read_temperature_sensor_im(self) -> float | None:
        if not self.nodes.temperature_sensor_im:
            return None
        return _as_float(self._snap.get(str(self.nodes.temperature_sensor_im)), default=None)


    def read_loading_door_status(self, idx: int) -> bool:
        if not self.nodes.loading_door_stat_fmt:
            return False
        nid = self.nodes.loading_door_stat_fmt.format(idx=int(idx))
        return _as_bool(self._snap.get(nid), default=False)

    def read_unloading_door_status(self, idx: int) -> bool:
        if not self.nodes.unloading_door_stat_fmt:
            return False
        nid = self.nodes.unloading_door_stat_fmt.format(idx=int(idx))
        return _as_bool(self._snap.get(nid), default=False)

    def read_loading_column_sensor(self, col: int) -> bool:
        # tick() не async, поэтому читаем из snapshot
        if not self.nodes.loading_col_sensor_fmt:
            return False
        nid = self.nodes.loading_col_sensor_fmt.format(col=int(col))
        return _as_bool(self._snap.get(nid), default=False)
    
    def read_unloading_column_sensor(self, col: int) -> bool:
        # tick() не async, поэтому читаем из snapshot
        if not self.nodes.unloading_col_sensor_fmt:
            return False
        nid = self.nodes.unloading_col_sensor_fmt.format(col=int(col))
        return _as_bool(self._snap.get(nid), default=False)


    # -----------------
    # door commands (pulses)
    # -----------------
    def request_open_loading_cell(self, idx: int) -> None:
        if not self.nodes.loading_door_cmd_fmt:
            return
        nid = self.nodes.loading_door_cmd_fmt.format(idx=int(idx))
        self._request_pulse(nid)

    def request_open_unloading_cell(self, idx: int) -> None:
        if not self.nodes.unloading_door_cmd_fmt:
            return
        nid = self.nodes.unloading_door_cmd_fmt.format(idx=int(idx))
        self._request_pulse(nid)

    def _request_pulse(self, node_id: str) -> None:
        # If already in flight, ignore (dedup)
        if node_id in self._pulses:
            return
        self._pulses[node_id] = 0

    async def _flush_pulses(self) -> None:
        if not self._pulses:
            return

        # one transition per poll per node
        to_delete = []
        for nid, stage in list(self._pulses.items()):
            if stage == 0:
                await self._write_dedup(nid, True)
                self._pulses[nid] = 1
            elif stage == 1:
                await self._write_dedup(nid, False)
                to_delete.append(nid)

        for nid in to_delete:
            self._pulses.pop(nid, None)

    # -----------------
    # stacklight
    # -----------------
    def request_stacklight_color(self, color: int) -> None:
        self._desired_stack_color_int = int(color)

    def request_stacklight_sound(self, sound: int) -> None:
        self._desired_stack_sound_int = int(sound)

    async def _flush_stacklight(self) -> None:
            """
            Stacklight with cmds:
              - color cmd  (int16): 0..6
              - sound cmd  (int16): 0..4
            """
            # color
            if self.nodes.stacklight_color and (self._desired_stack_color_int is not None):
                await self._write_dedup(self.nodes.stacklight_color, int(self._desired_stack_color_int))
        
            # sound
            if self.nodes.stacklight_sound and (self._desired_stack_sound_int is not None):
                await self._write_dedup(self.nodes.stacklight_sound, int(self._desired_stack_sound_int))

    # -----------------
    # low-level write
    # -----------------
    async def _write_dedup(self, node_id: str, value: Any) -> None:
        #nid = str(node_id)
    
        # --- BOOL всегда отдельно (иначе True/False попадают как int) ---
        if isinstance(value, bool):
            key = ("bool", bool(value))
            prev = self._last_written.get(node_id)
            if prev == key:
                return
            await self.svc.write(node_id, bool(value))
            self._last_written[node_id] = key
            return
    
        # --- dedup key ---
        if isinstance(value, int):
            key = ("int", int(value))
        elif isinstance(value, float):
            key = ("float", float(value))
        elif isinstance(value, str):
            key = ("str", value)
        else:
            key = (type(value).__name__, value)
    
        prev = self._last_written.get(node_id)
        if prev == key:
            return

        # --- stacklight v2: пишем только как типизированный Variant ---
        #color_nid = getattr(self.nodes.stacklight_color", None)
        #sound_nid = getattr(self, "nodes.stacklight_sound", None)
        is_stack_v2 = (node_id == self.nodes.stacklight_color) or (node_id == self.nodes.stacklight_sound)

        if is_stack_v2 and isinstance(value, int):
            # пробуем импорт ua из разных lib (у тебя по факту одна из них есть)
            try:
                from asyncua import ua as _ua  # type: ignore
            except Exception:
                try:
                    from opcua import ua as _ua  # type: ignore
                except Exception:
                    _ua = None  # type: ignore
    
            if _ua is not None:
                def is_type_mismatch(e: Exception) -> bool:
                    s = repr(e)
                    return ("BadTypeMismatch" in s) or ("BadTypeMismatch" in str(e))
    
                # если уже нашли рабочий тип для этой ноды — пробуем его первым
                vtypes = []
                vt0 = self._write_vtypes.get(node_id)
                if vt0 is not None:
                    vtypes.append(vt0)

                # самые частые PLC-типы (порядок важен)
                vtypes += [
                    _ua.VariantType.Int16,
                    _ua.VariantType.UInt16,
                    _ua.VariantType.Int32,
                    _ua.VariantType.UInt32,
                    _ua.VariantType.SByte,
                    _ua.VariantType.Byte,
                    _ua.VariantType.Int64,
                    _ua.VariantType.UInt64,
                ]
    
                last_err: Exception | None = None

                for vt in vtypes:
                    try:
                        await self.svc.write(node_id, _ua.Variant(int(value), vt))
                        self._write_vtypes[node_id] = vt
                        self._last_written[node_id] = key
                        return
                    except Exception as e:
                        last_err = e
                        if not is_type_mismatch(e):
                            raise
    
                # если все варианты дали BadTypeMismatch - пробрасываем
                raise last_err  # type: ignore[misc]
    
            # ua не смогли импортировать - fallback (скорее всего снова будет mismatch)
            await self.svc.write(node_id, int(value))
            self._last_written[node_id] = key
            return
    
        # --- default write for all other nodes ---
        await self.svc.write(node_id, value)
        self._last_written[node_id] = key


    # --- debug setters (for tests) ---
    def set_safety_ok(self, v: bool) -> None:
        # только для тестов: меняем локальный snapshot, который читают read_*()
        self._snap[self.nodes.safety_ok] = bool(v)



