from __future__ import annotations

from typing import Optional

from app.daemon.io_base import IOBase


class MockIO(IOBase):
    def __init__(self, *, safety_ok: bool = True, trash_present: bool = True, reject_bin_count_init: int = 0):
        self._safety_ok = bool(safety_ok)
        self._trash_present = bool(trash_present)
        self._reject_bin_count = int(reject_bin_count_init)

        # postamat snapshots (for tests)
        self._loading_door_stat: dict[int, bool] = {}
        self._unloading_door_stat: dict[int, bool] = {}
        self._loading_col_sensor: dict[int, bool] = {}
        self._unloading_col_sensor: dict[int, bool] = {}

        # last requested outputs
        self.last_stack_bools: Optional[tuple[bool, bool, bool, bool]] = None
        self.last_stack_state: Optional[int] = None
        self.open_loading_requests: list[int] = []
        self.open_unloading_requests: list[int] = []

    async def connect(self) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    async def poll_once(self) -> None:
        return None

    # setters for tests
    def set_safety_ok(self, v: bool) -> None:
        self._safety_ok = bool(v)

    def set_trashcan_present(self, v: bool) -> None:
        self._trash_present = bool(v)

    def set_loading_door_status(self, idx: int, v: bool) -> None:
        self._loading_door_stat[int(idx)] = bool(v)

    def set_unloading_door_status(self, idx: int, v: bool) -> None:
        self._unloading_door_stat[int(idx)] = bool(v)

    def set_loading_column_sensor(self, col: int, v: bool) -> None:
        self._loading_col_sensor[int(col)] = bool(v)

    def set_unloading_column_sensor(self, col: int, v: bool) -> None:
        self._unloading_col_sensor[int(col)] = bool(v)

    # reads
    def read_safety_ok(self) -> bool:
        return bool(self._safety_ok)

    def read_trashcan_present(self) -> bool:
        return bool(self._trash_present)

    #def read_reject_bin_count(self) -> Optional[int]:
    #    return int(self._reject_bin_count)

    #def set_reject_bin_count(self, v: int) -> None:
    #    self._reject_bin_count = int(v)

    def read_loading_door_status(self, idx: int) -> bool:
        return bool(self._loading_door_stat.get(int(idx), False))

    def read_unloading_door_status(self, idx: int) -> bool:
        return bool(self._unloading_door_stat.get(int(idx), False))

    def read_loading_column_sensor(self, col: int) -> bool:
        return bool(self._loading_col_sensor.get(int(col), False))

    def read_unloading_column_sensor(self, col: int) -> bool:
        return bool(self._unloading_col_sensor.get(int(col), False))

    # requests
    def request_stacklight_bools(self, *, sound: bool, green: bool, yellow: bool, red: bool) -> None:
        self.last_stack_bools = (bool(sound), bool(green), bool(yellow), bool(red))
        self.last_stack_state = None

    def request_stacklight_state(self, state: int) -> None:
        self.last_stack_state = int(state)
        self.last_stack_bools = None

    def request_open_loading_cell(self, idx: int) -> None:
        self.open_loading_requests.append(int(idx))

    def request_open_unloading_cell(self, idx: int) -> None:
        self.open_unloading_requests.append(int(idx))
