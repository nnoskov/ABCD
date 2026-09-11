from __future__ import annotations

from app.daemon.io_base import IOBase
from app.daemon.opcua_io import OpcUaIO


class HybridIO(IOBase):
    """Hybrid IO:
    - physical IO via OPCUA (postamat + safety + stacklight)
    - logical reject_bin_count stored locally
    """

    def __init__(self, *, opcua: OpcUaIO, reject_bin_count_init: int = 0):
        self.opcua = opcua
        self._reject_bin_count = int(reject_bin_count_init)

    async def connect(self) -> None:
        await self.opcua.connect()

    async def disconnect(self) -> None:
        await self.opcua.disconnect()

    async def poll_once(self) -> None:
        await self.opcua.poll_once()

    # --- core inputs ---
    def read_safety_ok(self) -> bool:
        return self.opcua.read_safety_ok()

    def read_safety_status(self) -> int | None:
        return self.opcua.read_safety_status()        

    def read_trashcan_present(self) -> bool:
        return self.opcua.read_trashcan_present()

    # --- temperature sensors (float, optional) ---
    def read_temperature_sensor_loading(self) -> float | None:
        return self.opcua.read_temperature_sensor_loading()

    def read_temperature_sensor_im(self) -> float | None:
        return self.opcua.read_temperature_sensor_im()

    # --- reject bin (logical) ---
    #def read_reject_bin_count(self) -> int:
    #    return int(self._reject_bin_count)

    #def set_reject_bin_count(self, v: int) -> None:
    #    self._reject_bin_count = int(v)

    # --- postamat statuses/sensors ---
    def read_loading_door_status(self, idx: int) -> bool:
        return self.opcua.read_loading_door_status(idx)

    def read_unloading_door_status(self, idx: int) -> bool:
        return self.opcua.read_unloading_door_status(idx)

    def read_loading_column_sensor(self, col: int) -> bool:
        return self.opcua.read_loading_column_sensor(col)

    def read_unloading_column_sensor(self, col: int) -> bool:
        return self.opcua.read_unloading_column_sensor(col)

    # --- postamat commands ---
    def request_open_loading_cell(self, idx: int) -> None:
        self.opcua.request_open_loading_cell(idx)

    def request_open_unloading_cell(self, idx: int) -> None:
        self.opcua.request_open_unloading_cell(idx)

    # --- stacklight ---
    #def request_stacklight_bools(self, *, sound: bool, green: bool, yellow: bool, red: bool) -> None:
    #    self.opcua.request_stacklight_bools(sound=sound, green=green, yellow=yellow, red=red)

    #def request_stacklight_state(self, state: int) -> None:
    #    self.opcua.request_stacklight_state(state)

    def request_stacklight_color(self, color: int) -> None:
        self.opcua.request_stacklight_color(color)

    def request_stacklight_sound(self, sound: int) -> None:
        self.opcua.request_stacklight_sound(sound)