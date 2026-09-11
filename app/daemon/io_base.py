from __future__ import annotations

from abc import ABC, abstractmethod


class IOBase(ABC):
    """
    Unified IO interface for Supervisor + daemon loop.

    Conventions:
      - connect()/disconnect()/poll_once() are async and called by daemon loop.
      - read_* are sync and used inside Supervisor.tick().
      - request_* are sync; backend flushes writes in poll_once().
    """
    # --- lifecycle ---
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def poll_once(self) -> None: ...

    # --- core inputs ---
    @abstractmethod
    def read_safety_ok(self) -> bool: ...

    def read_safety_status(self) -> int | None:
        return None    

    @abstractmethod
    def read_trashcan_present(self) -> bool: ...

    # --- temperature sensors (optional) ---
    @abstractmethod
    def read_temperature_sensor_loading(self) -> float: ...   # T_1

    @abstractmethod
    def read_temperature_sensor_im(self) -> float: ...              # T_2

    # --- reject bin (logical counter) ---
    #@abstractmethod
    #def read_reject_bin_count(self) -> int: ...

    #@abstractmethod
    #def set_reject_bin_count(self, v: int) -> None: ...

    # --- postamat sensors/statuses ---
    @abstractmethod
    def read_loading_door_status(self, idx: int) -> bool: ...

    @abstractmethod
    def read_unloading_door_status(self, idx: int) -> bool: ...

    @abstractmethod
    def read_loading_column_sensor(self, col: int) -> bool: ...

    @abstractmethod
    def read_unloading_column_sensor(self, col: int) -> bool: ...

    # --- postamat commands ---
    @abstractmethod
    def request_open_loading_cell(self, idx: int) -> None: ...

    @abstractmethod
    def request_open_unloading_cell(self, idx: int) -> None: ...

    # --- stacklight ---
    @abstractmethod
    def request_stacklight_color(self, color: int) -> None: ...

    @abstractmethod
    def request_stacklight_sound(self, sound: int) -> None: ...

###    @abstractmethod
###    def request_stacklight_bools(self, *, sound: bool, green: bool, yellow: bool, red: bool) -> None: ...
###
###    @abstractmethod
###    def request_stacklight_state(self, state: int) -> None: ...

