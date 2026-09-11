from __future__ import annotations
from collections import deque
from typing import Optional
from app.daemon.rtk_port import RTKPort, RTKSnapshot

class MockRTK(RTKPort):
    def __init__(self):
        self._q = deque()
        self._snap = RTKSnapshot(connected=True, state="ready", busy=False, error=None)
        self._last_speed = None
        self._last_meas = None
        self._last_calib = None
        self._manual_mode = False
        self._next_step_count = 0        

    def request(self, cmd: str, payload: Optional[dict] = None) -> None:
        self._q.append((cmd, payload or {}))

    def snapshot(self) -> RTKSnapshot:
        return self._snap

    async def poll_once(self) -> None:
        if not self._q:
            return
        cmd, payload = self._q.popleft()
        # простая модель
        if cmd == "pause":
            self._snap = RTKSnapshot(True, "paused", False, None)

        elif cmd == "resume":
            self._snap = RTKSnapshot(True, "ready", False, None)

        elif cmd == "stop":
            self._snap = RTKSnapshot(True, "stopped", False, None)

        elif cmd == "reset":
            self._snap = RTKSnapshot(True, "ready", False, None)

        elif cmd == "calibrate":
            self._snap = RTKSnapshot(True, "calibrating", True, None)
            self._snap = RTKSnapshot(True, "ready", False, None)

        elif cmd == "start":
            self._snap = RTKSnapshot(True, "running", True, None)
            self._snap = RTKSnapshot(True, "running", False, None)

        elif cmd == "setspeed":
            self._last_speed = payload.get("speed")
            # состояние не меняем

        elif cmd == "send_measurement_result":
            self._last_meas = payload

        elif cmd == "send_calibration_result":
            self._last_calib = payload

        elif cmd == "set_step_mode":
            self._manual_mode = bool(payload.get("manual", False))

        elif cmd == "next_step":
            if not self._manual_mode:
                self._snap = RTKSnapshot(
                    True,
                    self._snap.state,
                    self._snap.busy,
                    "next_step is available only in manual mode",
                )
            else:
                self._next_step_count += 1
                self._snap = RTKSnapshot(
                    True,
                    self._snap.state,
                    self._snap.busy,
                    None,
                )            

        else:
            self._snap = RTKSnapshot(True, self._snap.state, self._snap.busy, f"unknown cmd {cmd}")