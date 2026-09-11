from __future__ import annotations

import asyncio
import json
import logging

import urllib.parse
import urllib.request
import urllib.error
from collections import deque
from typing import Optional, Tuple, Dict, Any

from app.daemon.rtk_port import RTKPort, RTKSnapshot

log = logging.getLogger(__name__)


def _http_get_json(url: str, timeout: float = 2.0) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GET {url} -> HTTP {e.code} {e.reason}: {body[:500]}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"GET {url} -> URLError: {e.reason}") from e


def _http_post_json(
    url: str, data: Dict[str, Any], timeout: float = 2.0
) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return parsed
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"POST {url} -> HTTP {e.code} {e.reason}: {body[:500]}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"POST {url} -> URLError: {e.reason}") from e


def _http_post_empty(url: str, timeout: float = 2.0) -> Dict[str, Any]:
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return parsed
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"POST {url} -> HTTP {e.code} {e.reason}: {body[:500]}"
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"POST {url} -> URLError: {e.reason}") from e


def _is_ok(resp: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if not isinstance(resp, dict):
        return False, "bad response type"

    code = resp.get("Code", resp.get("code"))
    if code is not None:
        try:
            if int(code) < 0:
                return False, str(
                    resp.get("Reason") or resp.get("reason") or f"code={code}"
                )
        except Exception:
            pass

    status = resp.get("Status", resp.get("status"))
    # если RTK не присылает Status - считаем ответ успешным
    if status is None:
        return True, None

    if str(status).upper() != "OK":
        return False, str(
            resp.get("Reason") or resp.get("reason") or f"status={status}"
        )

    return True, None


def _lower_bool(v: bool) -> str:
    return "true" if bool(v) else "false"


class HttpRTK(RTKPort):
    """
    RTK_api.md
    """

    def __init__(
        self, base_url: str, prefix: str = "api/master", timeout_sec: float = 2.0
    ):
        self.base_url = (base_url or "").strip().rstrip("/")
        self.prefix = (prefix or "").strip().strip("/")
        self.timeout = float(timeout_sec)

        base = f"{self.base_url}/{self.prefix}"

        self.url_data = f"{base}/data"
        self.url_start = f"{base}/start"
        self.url_pause = f"{base}/pause"
        self.url_resume = f"{base}/resume"
        self.url_stop = f"{base}/stop"
        self.url_reset = f"{base}/reset"
        self.url_set_speed = f"{base}/set_speed"
        self.url_meas_res = f"{base}/measurement_result"
        self.url_calib_res = f"{base}/etalon_result"
        self.url_check_etalon = f"{base}/check_etalon"
        self.url_sensor_state = f"{base}/sensor_state"
        self.url_cycle_on = f"{base}/cycle_on"
        self.url_reset_defect_counter = f"{base}/reset_defect_counter"
        self.url_set_step_mode = f"{base}/set_step_mode"
        self.url_next_step = f"{base}/next_step"

        self._q = deque()
        self._snap = RTKSnapshot(
            connected=False,
            state="unknown",
            busy=False,
            error=None,
            air_pressure_ok=None,
        )

    def request(self, cmd: str, payload: Optional[dict] = None) -> None:
        payload = payload or {}
        self._q.append((cmd, payload))

    def snapshot(self) -> RTKSnapshot:
        return self._snap

    async def poll_once(self) -> None:
        connected = False
        state_str = "unknown"
        busy = False
        err_txt = None
        pc = None
        dc = None
        rs = None

        # 1) status
        try:
            raw = await asyncio.to_thread(_http_get_json, self.url_data, self.timeout)
            ok, err = _is_ok(raw)
            if not ok:
                self._snap = RTKSnapshot(
                    connected=False,
                    state="error",
                    busy=False,
                    error=err,
                    pickcount=None,
                    defectcount=None,
                    robot_state=None,
                    air_pressure_ok=None,
                )
                return

            data = raw.get("Data") or {}
            r1 = data.get("rs013n") or {}
            r2 = data.get("rs007l") or {}

            def _to_bool(v):
                if isinstance(v, bool):
                    return v
                if v is None:
                    return None
                s = str(v).strip().lower()
                if s in ("1", "true", "yes", "y", "on"):
                    return True
                if s in ("0", "false", "no", "n", "off"):
                    return False
                return None

            def _get_ci(d: Dict[str, Any], *keys: str):
                """
                Нужен для Data/io/DI09 и Data/io/DI10,
                чтобы пережить варианты регистра ключей.
                """
                if not isinstance(d, dict):
                    return None

                for key in keys:
                    if key in d:
                        return d[key]

                wanted = {str(k).lower() for k in keys}
                for k, v in d.items():
                    if str(k).lower() in wanted:
                        return v

                return None

            io_data = _get_ci(data, "io", "IO") or {}

            air_pressure_raw = _get_ci(io_data, "DI09", "di09")
            if air_pressure_raw is None:
                # запасной вариант, если РТК отдаст плоский ключ
                air_pressure_raw = _get_ci(data, "DI09", "di09", "data/io/DI09")

            air_pressure_ok = _to_bool(air_pressure_raw)

            # Если в одном snapshot DI09 не пришёл/не распарсился,
            # не создаём ложный air NOK. Оставляем последнее известное значение.
            if air_pressure_ok is None:
                air_pressure_ok = getattr(self._snap, "air_pressure_ok", None)

            positioner_part_raw = _get_ci(io_data, "DI10", "di10")
            if positioner_part_raw is None:
                # Запасной вариант, если РТК отдаст плоский ключ.
                positioner_part_raw = _get_ci(
                    data,
                    "DI10",
                    "di10",
                    "data/io/DI10",
                )

            # True  = деталь присутствует на позиционере.
            # False = деталь отсутствует.
            # None  = значение не пришло или не распознано.
            positioner_part_present = _to_bool(positioner_part_raw)

            c1 = _to_bool(_get_ci(r1, "connected"))
            c2 = _to_bool(_get_ci(r2, "connected"))
            cs1 = _to_bool(_get_ci(r1, "cs"))
            cs2 = _to_bool(_get_ci(r2, "cs"))

            # Условия, от которых зависит переход cs в True после cycle_on.
            # r1 = Data/rs013n, r2 = Data/rs007l.
            teach_r1 = _to_bool(_get_ci(r1, "teach"))
            teach_r2 = _to_bool(_get_ci(r2, "teach"))
            teachl_r1 = _to_bool(_get_ci(r1, "teachl"))
            teachl_r2 = _to_bool(_get_ci(r2, "teachl"))
            tpemg_r1 = _to_bool(_get_ci(r1, "tpemg"))
            tpemg_r2 = _to_bool(_get_ci(r2, "tpemg"))
            opemg_r1 = _to_bool(_get_ci(r1, "opemg"))
            opemg_r2 = _to_bool(_get_ci(r2, "opemg"))
            exemg_r1 = _to_bool(_get_ci(r1, "exemg"))
            exemg_r2 = _to_bool(_get_ci(r2, "exemg"))
            robot_error_r1 = _to_bool(_get_ci(r1, "error"))
            robot_error_r2 = _to_bool(_get_ci(r2, "error"))

            watchdog_r1 = _to_bool(_get_ci(r1, "watchdog"))
            watchdog_r2 = _to_bool(_get_ci(r2, "watchdog"))

            def _to_int(v):
                try:
                    return int(v)
                except Exception:
                    return None

            ecode_r1 = _to_int(_get_ci(r1, "ecode"))
            ecode_r2 = _to_int(_get_ci(r2, "ecode"))

            pc = _to_int(r2.get("pickcount"))
            dc = _to_int(r2.get("defectcount"))
            rs = _to_int(r2.get("state"))

            tarein = _to_int(r1.get("tarein"))
            tareout = _to_int(r1.get("tareout"))
            putcount = _to_int(r2.get("putcount"))

            connected = bool(c1) and bool(c2)  # or

            # busy: state > 0 и !=255
            def _busy(r: Dict[str, Any]) -> bool:
                try:
                    st = int(r.get("state", 0))
                except Exception:
                    st = 0
                return st > 0 and st != 255

            busy = _busy(r1) or _busy(r2)

            action2 = str(r2.get("action") or "")
            action1 = str(r1.get("action") or "")

            a1 = action1.strip().lower()
            a2 = action2.strip().lower()

            # 1) если r2 ждёт результат - показываем r2
            if a2 in ("waitingmmresult", "waitingcalibrationresult"):
                state_str = action2
            # 2) иначе сенсоры r1 можно показывать
            elif a1 in ("waitoutstockersensor", "waitinstockersensor"):
                state_str = action1
            else:
                state_str = action2 or action1 or "unknown"

            # error
            err_txt = None
            if robot_error_r1 is True or robot_error_r2 is True:
                err_txt = f"robot_error r1={ecode_r1} " f"r2={ecode_r2}"
            else:
                # state==255 - требуются действия оператора
                if str(r1.get("state")) == "255" or str(r2.get("state")) == "255":
                    err_txt = "robot_state_255"

            self._snap = RTKSnapshot(
                connected=connected,
                state=state_str,
                busy=busy,
                error=err_txt,
                pickcount=pc,
                defectcount=dc,
                robot_state=rs,
                action_r1=a1 or None,
                action_r2=a2 or None,
                connected_r1=c1,
                connected_r2=c2,
                cs_r1=cs1,
                cs_r2=cs2,
                teach_r1=teach_r1,
                teach_r2=teach_r2,
                teachl_r1=teachl_r1,
                teachl_r2=teachl_r2,
                tpemg_r1=tpemg_r1,
                tpemg_r2=tpemg_r2,
                opemg_r1=opemg_r1,
                opemg_r2=opemg_r2,
                exemg_r1=exemg_r1,
                exemg_r2=exemg_r2,
                robot_error_r1=robot_error_r1,
                robot_error_r2=robot_error_r2,
                ecode_r1=ecode_r1,
                ecode_r2=ecode_r2,
                watchdog_r1=watchdog_r1,
                watchdog_r2=watchdog_r2,
                tarein=tarein,
                tareout=tareout,
                putcount=putcount,
                air_pressure_ok=air_pressure_ok,
                positioner_part_present=positioner_part_present,
            )
        except Exception as e:
            self._snap = RTKSnapshot(
                connected=False,
                state="error",
                busy=False,
                error=repr(e),
                pickcount=None,
                defectcount=None,
                robot_state=None,
                air_pressure_ok=None,
            )
            return

        # 2) commands (one per tick, если RTK busy - не шлём)
        if not self._q:
            return

        cmd, payload = self._q[0]

        # даже если busy - разрешаем "результатные" команды, если RTK ждёт их
        always_allowed = cmd in (
            "start",
            "pause",
            "resume",
            "stop",
            "reset",
            "setspeed",
            "sensor_state",
            "cycle_on",
            "calibrate",
            "reset_defect_counter",
            "set_step_mode",
            "next_step",
        )
        waiting = str(self._snap.state or "").strip().lower() in (
            "waitingmmresult",
            "waitingcalibrationresult",
            "waitinstockersensor",
            "waitoutstockersensor",
        )
        result_allowed = (
            cmd in ("send_measurement_result", "send_calibration_result") and waiting
        )

        if self._snap.busy and not (always_allowed or result_allowed):
            return

        cmd, payload = self._q.popleft()

        try:
            await self._execute(cmd, payload)
        except Exception as e:
            self._snap = RTKSnapshot(
                connected=self._snap.connected,
                state=self._snap.state,
                busy=self._snap.busy,
                error=repr(e),
                pickcount=getattr(self._snap, "pickcount", None),
                defectcount=getattr(self._snap, "defectcount", None),
                robot_state=getattr(self._snap, "robot_state", None),
                action_r1=getattr(self._snap, "action_r1", None),
                action_r2=getattr(self._snap, "action_r2", None),
                connected_r1=getattr(self._snap, "connected_r1", None),
                connected_r2=getattr(self._snap, "connected_r2", None),
                cs_r1=getattr(self._snap, "cs_r1", None),
                cs_r2=getattr(self._snap, "cs_r2", None),
                teach_r1=getattr(self._snap, "teach_r1", None),
                teach_r2=getattr(self._snap, "teach_r2", None),
                teachl_r1=getattr(self._snap, "teachl_r1", None),
                teachl_r2=getattr(self._snap, "teachl_r2", None),
                tpemg_r1=getattr(self._snap, "tpemg_r1", None),
                tpemg_r2=getattr(self._snap, "tpemg_r2", None),
                opemg_r1=getattr(self._snap, "opemg_r1", None),
                opemg_r2=getattr(self._snap, "opemg_r2", None),
                exemg_r1=getattr(self._snap, "exemg_r1", None),
                exemg_r2=getattr(self._snap, "exemg_r2", None),
                robot_error_r1=getattr(self._snap, "robot_error_r1", None),
                robot_error_r2=getattr(self._snap, "robot_error_r2", None),
                ecode_r1=getattr(self._snap, "ecode_r1", None),
                ecode_r2=getattr(self._snap, "ecode_r2", None),
                watchdog_r1=getattr(self._snap, "watchdog_r1", None),
                watchdog_r2=getattr(self._snap, "watchdog_r2", None),
                tarein=getattr(self._snap, "tarein", None),
                tareout=getattr(self._snap, "tareout", None),
                putcount=getattr(self._snap, "putcount", None),
                air_pressure_ok=getattr(self._snap, "air_pressure_ok", None),
                positioner_part_present=getattr(
                    self._snap, "positioner_part_present", None
                ),
            )

    async def _execute(self, cmd: str, payload: dict) -> None:
        # cmd strings приходят от Supervisor: start/setspeed/send_measurement_result/send_calibration_result/pause/resume/stop/reset/calibrate

        if cmd == "pause":
            resp = await asyncio.to_thread(
                _http_post_empty, self.url_pause, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "resume":
            resp = await asyncio.to_thread(
                _http_post_empty, self.url_resume, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "stop":
            resp = await asyncio.to_thread(
                _http_post_empty, self.url_stop, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "reset":
            resp = await asyncio.to_thread(
                _http_post_empty, self.url_reset, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "reset_defect_counter":
            resp = await asyncio.to_thread(
                _http_post_empty, self.url_reset_defect_counter, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "cycle_on":
            resp = await asyncio.to_thread(
                _http_post_empty, self.url_cycle_on, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "set_step_mode":
            manual = bool(payload.get("manual", False))

            # API РТК:
            # mode=false — ручной пошаговый режим;
            # mode=true  — обычный автоматический режим.
            # rtk_mode = not manual
            rtk_mode = manual

            qs = urllib.parse.urlencode(
                {
                    "mode": _lower_bool(rtk_mode),
                }
            )
            url = f"{self.url_set_step_mode}?{qs}"

            resp = await asyncio.to_thread(
                _http_post_empty,
                url,
                self.timeout,
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "next_step":
            resp = await asyncio.to_thread(
                _http_post_empty,
                self.url_next_step,
                self.timeout,
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "start":
            body = {
                "ProductName": payload.get("ProductName", payload.get("product_name")),
                "ProductSpec": payload.get(
                    "ProductSpec", payload.get("product_spec", 0)
                ),
                "ProductCount": payload.get(
                    "ProductCount", payload.get("product_count")
                ),
                "InTareIDs": payload.get("InTareIDs", payload.get("in_tare_ids", [])),
                "OutTareIDs": payload.get(
                    "OutTareIDs", payload.get("out_tare_ids", [])
                ),
                "Layout": int(payload.get("Layout", payload.get("layout", 0))),
                "GlobalMaxTareCount": int(
                    payload.get(
                        "GlobalMaxTareCount", payload.get("GlobalMaxTareCount", 0)
                    )
                ),
                "CurrentMaxTareCount": int(
                    payload.get(
                        "CurrentMaxTareCount", payload.get("CurrentMaxTareCount", 0)
                    )
                ),
                "UseAlternateWave": int(
                    payload.get(
                        "UseAlternateWave", payload.get("use_alternate_wave", False)
                    )
                ),
            }
            resp = await asyncio.to_thread(
                _http_post_json, self.url_start, body, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "setspeed":
            speed = payload.get("speed", payload.get("value"))
            if speed is None:
                raise RuntimeError("speed is required")
            qs = urllib.parse.urlencode({"speed": int(speed)})
            log.warning("SPEED qs=%s", qs)

            url = f"{self.url_set_speed}?{qs}"
            log.warning("URL %s", url)

            resp = await asyncio.to_thread(_http_post_empty, url, self.timeout)
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "send_measurement_result":
            # /measurement_result?result=true|false
            result = payload.get("result", payload.get("ok"))
            if result is None:
                raise RuntimeError("result/ok is required")
            qs = urllib.parse.urlencode({"result": _lower_bool(bool(result))})
            url = f"{self.url_meas_res}?{qs}"
            resp = await asyncio.to_thread(_http_post_empty, url, self.timeout)
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "send_calibration_result":
            # /calibration_result?result=0|-1|-2
            result = payload.get("result")
            if result is None and "ok" in payload:
                result = 0 if bool(payload["ok"]) else -1
            if result is None:
                raise RuntimeError("result is required")
            qs = urllib.parse.urlencode({"result": int(result)})
            url = f"{self.url_calib_res}?{qs}"
            resp = await asyncio.to_thread(_http_post_empty, url, self.timeout)
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "calibrate":
            # /check_etalon?etalon_id=0
            etalon_id = int(payload.get("etalon_id", 0))
            qs = urllib.parse.urlencode({"etalon_id": etalon_id})
            url = f"{self.url_check_etalon}?{qs}"
            resp = await asyncio.to_thread(_http_post_empty, url, self.timeout)
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        if cmd == "sensor_state":
            name = payload.get("SensorName", payload.get("sensor_name"))
            state = payload.get("State")
            if not name:
                raise RuntimeError("SensorName is required")

            body = {"SensorName": str(name), "State": bool(state)}
            resp = await asyncio.to_thread(
                _http_post_json, self.url_sensor_state, body, self.timeout
            )
            ok, err = _is_ok(resp)
            if not ok:
                raise RuntimeError(err)
            return

        raise RuntimeError(f"unknown RTK cmd: {cmd}")
