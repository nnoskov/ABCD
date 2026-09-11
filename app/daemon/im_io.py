from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Callable

from asyncua import ua

from app.daemon.asyncua_service import AsyncuaService


@dataclass(frozen=True)
class ImNodes:
    # outputs / commands
    vacuum: Optional[str] = None                  # bool
    calibrate: str = ""                           # SByte pulse: 0/1 then 0
    measure_start: Optional[str] = None           # bool (pulse)
    clear_db: Optional[str] = None    

    # inputs / statuses
    vacuum_state: Optional[str] = None            # bool
    remaining_calib_iter: str = ""                # sbyte/int
    measure_state: Optional[str] = None           # int/bool (0 -> done)

    # program selection (method call)
    scheme_iface: Optional[str] = None            # object node id
    scheme_load_method: Optional[str] = None      # method node id

    @staticmethod
    def from_env() -> "ImNodes":
        return ImNodes(
            vacuum=os.getenv("IM_NODE_VACUUM"),
            vacuum_state=os.getenv("IM_NODE_VACUUM_STATE"),
            calibrate=os.getenv("IM_NODE_CALIBRATE", ""),
            remaining_calib_iter=os.getenv("IM_NODE_REMAINING_CALIB_ITER", ""),
            measure_start=os.getenv("IM_NODE_MEASURE_START"),
            clear_db=os.getenv("IM_NODE_CLEAR_DB"),            
            measure_state=os.getenv("IM_NODE_MEASURE_STATE"),
            scheme_iface=os.getenv("IM_NODE_SCHEME_IFACE"),
            scheme_load_method=os.getenv("IM_NODE_SCHEME_LOAD_METHOD"),
        )


class ImOpcUaIO:
    """
    Драйвер ИМ.

    ВАЖНО:
    - Вакуум НЕ "зашит" в измерение/калибровку: им управляет внешний оркестратор (smoke/supervisor).
    - calibrate, measure_start, clear_db пишутся импульсно (True на 1 poll, затем False).
    - Результаты измерения читаются из объектов по имени:
        '{product_code}-res' (float значения)
        '{product_code}-tol' (bool ok/nok)
    """

    def __init__(
        self,
        *,
        endpoint: str,
        nodes: ImNodes,
        svc: Optional[AsyncuaService] = None,
        sub_interval_ms: int = 200,
        read_fallback_sec: float = 0.5,
        reconnect_backoff_sec: float = 1.0,
    ):
        self.endpoint = endpoint
        self.nodes = nodes
        self.sub_interval_ms = int(sub_interval_ms)
        self.read_fallback_sec = float(read_fallback_sec)
        self.reconnect_backoff_sec = float(reconnect_backoff_sec)

        self.svc: AsyncuaService = svc or AsyncuaService(
            endpoint=endpoint,
            sub_interval_ms=self.sub_interval_ms,
            reconnect_backoff_sec=self.reconnect_backoff_sec,
        )

        self._connected = False

        # cached inputs
        self._vacuum_state: bool = False
        self._remaining_calib_iter: int = 0
        self._measure_state: int = 0
        self._last_read_ts: float = 0.0

        # pending outputs
        self._desired_vacuum: Optional[bool] = None
        self._last_written_vacuum: Optional[bool] = None

        self._meas_pulse_phase: int = 0      # 0 none, 1->True, 2->False
        self._clear_db_pulse_phase: int = 0   # 0 none, 1->True, 2->False        
     
        self._calib_cmd_pending: bool = False
        self._calib_cmd_value: int = 0   # SByte

        # browse caches for results
        self._obj_cache: Dict[str, Any] = {}             # browseName -> Node
        self._vars_cache: Dict[str, Dict[str, Any]] = {} # obj_nodeid -> {key: Node}

        self._last_error: Optional[str] = None


    @property
    def connected(self) -> bool:
        return bool(self._connected and getattr(self.svc, "connected", False))
    
    @property
    def last_error(self) -> str | None:
        err = self._last_error or getattr(self.svc, "last_error", None)
        return str(err) if err else None


    async def connect(self) -> None:
        if self.connected:
            return
    
        try:
            await self.svc.connect()

            self._obj_cache.clear()
            self._vars_cache.clear()
            self._last_written_vacuum = None

            await self._read_inputs_once()
    
            if self.nodes.vacuum_state:
                await self.svc.subscribe_data_change(self.nodes.vacuum_state, self._on_vacuum_state)
            if self.nodes.remaining_calib_iter:
                await self.svc.subscribe_data_change(self.nodes.remaining_calib_iter, self._on_remaining_calib_iter)
            if self.nodes.measure_state:
                await self.svc.subscribe_data_change(self.nodes.measure_state, self._on_measure_state)
    
            self._connected = True
            self._last_error = None
            self._last_read_ts = time.monotonic()
    
        except Exception as e:
            self._connected = False
            self._last_error = repr(e)
            try:
                await self.svc.disconnect()
            except Exception:
                pass
            raise


    async def disconnect(self) -> None:
        self._connected = False
        self._obj_cache.clear()
        self._vars_cache.clear()
        self._last_written_vacuum = None        
        await self.svc.disconnect()


    async def poll_once(self) -> None:
        if not self.connected:
            try:
                await self.connect()
            except Exception as e:
                self._connected = False
                self._last_error = repr(e)
                return
    
        try:
            await self._apply_outputs()
    
            now = time.monotonic()
            if self.read_fallback_sec <= 0 or (now - self._last_read_ts) >= self.read_fallback_sec:
                self._last_read_ts = now
                await self._read_inputs_once()
    
        except Exception as e:
            self._connected = False
            self._last_error = repr(e)
            try:
                await self.svc.disconnect()
            except Exception:
                pass

    # ---------------- inputs ----------------

    def read_vacuum_state(self) -> bool:
        return bool(self._vacuum_state)

    def read_remaining_calib_iter(self) -> int:
        return int(self._remaining_calib_iter)

    def read_measure_state(self) -> int:
        return int(self._measure_state)

    # ---------------- output requests ----------------

    def request_vacuum(self, on: bool) -> None:
        self._desired_vacuum = bool(on)


    def request_calibrate(self, mode: int) -> None:
        # пишем ОДИН раз по команде; пока pending не отработал - новые запросы игнорируем
        if getattr(self, "_calib_cmd_pending", False):
            return
    
        v = int(mode)
        if v not in (1, 2):
            return  # невалидный режим
    
        self._calib_cmd_value = v
        self._calib_cmd_pending = True


    def request_start_measurement(self) -> None:
        if self._meas_pulse_phase == 0:
            self._meas_pulse_phase = 1


    def request_clear_db(self) -> None:
        if self._clear_db_pulse_phase == 0:
            self._clear_db_pulse_phase = 1            

    # ---------------- program selection ----------------

    async def load_program(self, product_code: str) -> bool:
        """
        Выбор измерительной программы.
        LoadScheme("{product_code}.json").
        """
        if not self.nodes.scheme_iface or not self.nodes.scheme_load_method:
            return False
        try:
            await self.svc.call_method(
                self.nodes.scheme_iface,
                self.nodes.scheme_load_method,
                f"{product_code}.json",
            )
            return True
        
        except Exception as e:
            self._connected = False
            self._last_error = repr(e)
            try:
                await self.svc.disconnect()
            except Exception:
                pass
            return False

    # ---------------- results ----------------

    async def read_measurement_results(self, product_code: str) -> Tuple[Dict[str, float], Dict[str, bool]]:
        """
        '{product_code}-res' -> float
        '{product_code}-tol' -> bool
        """
        c = getattr(self.svc, "_client", None)
        if c is None:
            return {}, {}

        res_obj = await self._resolve_object_by_browse_name(f"{product_code}-res")
        tol_obj = await self._resolve_object_by_browse_name(f"{product_code}-tol")
        if res_obj is None or tol_obj is None:
            return {}, {}

        values = await self._read_var_tree(res_obj, coerce="float")
        tols = await self._read_var_tree(tol_obj, coerce="bool")
        return values, tols

    # ---------------- wait helpers (for smoke/supervisor) ----------------

    async def wait_calibration_done(
        self,
        *,
        timeout_sec: float = 180.0,
        poll_period_sec: float = 0.2,
        stable_polls: int = 3,
        on_progress: Optional[Callable[[int], None]] = None,#
    ) -> Optional[int]:
        """
        Ждем завершение калибровки корректно:
        - "старт" = remainingCalibIter изменился относительно initial
        - во время процесса remainingCalibIter обычно > 0
        - "финиш" = remainingCalibIter стал 0 / -1 / -2 / -3 / -4 (после старта)
        """
        start_ts = time.monotonic()
        initial = int(self.read_remaining_calib_iter())
        started = False
        saw_positive = False
    
        terminal = {0, -1, -2, -3, -4}
        term_val: Optional[int] = None
        term_stable = 0
        
        last_emitted: Optional[int] = None

        while (time.monotonic() - start_ts) < timeout_sec:
            await self.poll_once()

            if not self.connected:
                return None
            
            cur = int(self.read_remaining_calib_iter())
    
            if on_progress is not None and cur != last_emitted:#
                last_emitted = cur#
                on_progress(cur)#

            if not started:
                if cur != initial:
                    started = True
    
            if started:
                if cur > 0:
                    saw_positive = True
    
                # Фиксируем терминал только если мы уже точно были "в процессе"
                if saw_positive and cur in terminal:
                    if term_val == cur:
                        term_stable += 1
                    else:
                        term_val = cur
                        term_stable = 1
    
                    if term_stable >= stable_polls:
                        return int(cur)
    
            await asyncio.sleep(poll_period_sec)
    
        return None


    async def wait_measure_done_and_read(
        self,
        product_code: str,
        *,
        timeout_sec: float = 60.0,
        poll_period_sec: float = 0.2,
        settle_after_done_sec: float = 0.2,
    ) -> Tuple[Dict[str, float], Dict[str, bool]]:
        """
        Ждём завершение измерения:
        - started когда measure_state != 0
        - done когда measure_state == 0 ПОСЛЕ старта
        - затем читаем результаты по product_code
        """
        start_ts = time.monotonic()
        started = False

        while (time.monotonic() - start_ts) < timeout_sec:
            await self.poll_once()

            if not self.connected:
                return {}, {}

            ms = self.read_measure_state()
            if not started and ms != 0:
                started = True
            if started and ms == 0:
                if settle_after_done_sec > 0:
                    await asyncio.sleep(settle_after_done_sec)
                return await self.read_measurement_results(product_code)
            await asyncio.sleep(poll_period_sec)

        return {}, {}

    # ---------------- internals ----------------

    async def _apply_outputs(self) -> None:
        # vacuum (dedup). Если write не прошёл, НЕ считаем состояние записанным:
        # после reconnect команда будет переотправлена.
        if self.nodes.vacuum and self._desired_vacuum is not None:
            if self._last_written_vacuum != self._desired_vacuum:
                ok = await self.svc.write_value(
                    self.nodes.vacuum,
                    bool(self._desired_vacuum),
                )
                if not ok:
                    raise RuntimeError("IM vacuum write failed")
                self._last_written_vacuum = bool(self._desired_vacuum)

        # calibrate one-shot (SByte): pending сбрасываем только после успешной записи.
        if self.nodes.calibrate and getattr(self, "_calib_cmd_pending", False):
            v = int(getattr(self, "_calib_cmd_value", 0))
            ok = await self.svc.write_value(
                self.nodes.calibrate,
                ua.Variant(v, ua.VariantType.SByte),
            )
            if not ok:
                raise RuntimeError("IM calibrate write failed")
            self._calib_cmd_pending = False

        # measure start pulse: фазу двигаем только после успешной записи.
        if self.nodes.measure_start and self._meas_pulse_phase in (1, 2):
            phase = int(self._meas_pulse_phase)
            ok = await self.svc.write_value(
                self.nodes.measure_start,
                True if phase == 1 else False,
            )
            if not ok:
                raise RuntimeError("IM measure_start write failed")
            self._meas_pulse_phase = 2 if phase == 1 else 0

        # Очистка журнала измерений: bool-импульс True -> False.
        # Фазу двигаем только после успешной записи.
        if self.nodes.clear_db and self._clear_db_pulse_phase in (1, 2):
            phase = int(self._clear_db_pulse_phase)

            ok = await self.svc.write_value(
                self.nodes.clear_db,
                True if phase == 1 else False,
            )

            if not ok:
                raise RuntimeError("IM clear_db write failed")

            self._clear_db_pulse_phase = 2 if phase == 1 else 0


    async def _read_inputs_once(self) -> None:
        try:
            if self.nodes.vacuum_state:
                self._vacuum_state = bool(await self.svc.read_value(self.nodes.vacuum_state, default=self._vacuum_state))
        except Exception:
            pass

        try:
            if self.nodes.remaining_calib_iter:
                self._remaining_calib_iter = int(
                    await self.svc.read_value(self.nodes.remaining_calib_iter, default=self._remaining_calib_iter)
                )
        except Exception:
            pass

        try:
            if self.nodes.measure_state:
                ms = await self.svc.read_value(self.nodes.measure_state, default=self._measure_state)
                self._measure_state = int(ms) if not isinstance(ms, bool) else (1 if ms else 0)
        except Exception:
            pass

    def _on_vacuum_state(self, value: Any) -> None:
        try:
            self._vacuum_state = bool(value)
        except Exception:
            pass

    def _on_remaining_calib_iter(self, value: Any) -> None:
        try:
            self._remaining_calib_iter = int(value)
        except Exception:
            pass

    def _on_measure_state(self, value: Any) -> None:
        try:
            self._measure_state = int(value) if not isinstance(value, bool) else (1 if value else 0)
        except Exception:
            pass

    async def _resolve_object_by_browse_name(self, browse_name: str):
        if browse_name in self._obj_cache:
            return self._obj_cache[browse_name]

        c = getattr(self.svc, "_client", None)
        if c is None:
            return None

        q = [c.nodes.objects]
        seen = set()

        while q:
            node = q.pop(0)
            try:
                nid = node.nodeid.to_string()
            except Exception:
                nid = str(node)
            if nid in seen:
                continue
            seen.add(nid)

            try:
                descs = await node.get_children_descriptions()
            except Exception:
                continue

            for d in descs:
                try:
                    bn = d.BrowseName.Name
                except Exception:
                    continue
                child = c.get_node(d.NodeId)

                if bn == browse_name and d.NodeClass == ua.NodeClass.Object:
                    self._obj_cache[browse_name] = child
                    return child

                if d.NodeClass == ua.NodeClass.Object:
                    q.append(child)

        return None

    async def _read_var_tree(self, obj_node, *, coerce: str) -> Dict[str, Any]:
        c = getattr(self.svc, "_client", None)
        if c is None:
            return {}

        try:
            cache_key = obj_node.nodeid.to_string()
        except Exception:
            cache_key = str(obj_node)

        if cache_key not in self._vars_cache:
            self._vars_cache[cache_key] = await self._collect_vars(obj_node, prefix="")

        vars_map = self._vars_cache[cache_key]
        if not vars_map:
            return {}

        nodes = list(vars_map.values())
        names = list(vars_map.keys())

        try:
            vals = await c.read_values(nodes)
        except Exception:
            vals = []
            for n in nodes:
                try:
                    vals.append(await n.read_value())
                except Exception:
                    vals.append(None)

        out: Dict[str, Any] = {}
        for nm, v in zip(names, vals):
            if coerce == "float":
                try:
                    out[nm] = float(v)
                except Exception:
                    continue
            elif coerce == "bool":
                try:
                    out[nm] = bool(v)
                except Exception:
                    continue
            else:
                out[nm] = v
        return out

    async def _collect_vars(self, node, *, prefix: str) -> Dict[str, Any]:
        c = getattr(self.svc, "_client", None)
        if c is None:
            return {}

        out: Dict[str, Any] = {}
        try:
            descs = await node.get_children_descriptions()
        except Exception:
            return out

        for d in descs:
            try:
                bn = d.BrowseName.Name
            except Exception:
                continue

            child = c.get_node(d.NodeId)
            p = f"{prefix}.{bn}" if prefix else str(bn)

            if d.NodeClass == ua.NodeClass.Variable:
                out[p] = child
            elif d.NodeClass == ua.NodeClass.Object:
                out.update(await self._collect_vars(child, prefix=p))

        return out
