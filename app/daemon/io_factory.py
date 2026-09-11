from __future__ import annotations

import os
from typing import List, Optional, Sequence

from app.daemon.hybrid_io import HybridIO
from app.daemon.opcua_io import OpcUaIO, OpcUaNodes

# MockIO (используется в тестах supervisor/scenarios)
from app.daemon.io import MockIO


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if (v is not None and v != "") else default


def _env_bool(key: str, default: bool = False) -> bool:
    v = _env(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    v = _env(key)
    return int(v) if v is not None else int(default)


def _parse_int_list(spec: Optional[str], default: Sequence[int]) -> List[int]:
    if not spec:
        return list(default)

    out: List[int] = []
    for part in spec.split(","):
        p = part.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            a_i = int(a.strip())
            b_i = int(b.strip())
            step = 1 if b_i >= a_i else -1
            out.extend(list(range(a_i, b_i + step, step)))
        else:
            out.append(int(p))

    # unique + keep order
    seen = set()
    res: List[int] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res


def _make_postamat_opcua_io() -> OpcUaIO:
    endpoint_post = _env("OPCUA_ENDPOINT_POSTAMAT") or _env("OPCUA_ENDPOINT")
    if not endpoint_post:
        raise RuntimeError("Missing OPCUA_ENDPOINT_POSTAMAT (or OPCUA_ENDPOINT)")

    # IM endpoint (учитываем, но подключим в пункте в ИМ)
    _ = _env("OPCUA_ENDPOINT_IM")

    timeout_sec = float(_env("OPCUA_TIMEOUT_SEC", "2.0"))
    sub_ms = int(_env("OPCUA_SUB_PERIOD_MS", "200"))
    read_fallback_sec = float(_env("OPCUA_READ_FALLBACK_SEC", "2.0"))
    backoff = float(_env("OPCUA_RECONNECT_BACKOFF_SEC", "1.0"))
    backoff_max = float(_env("OPCUA_RECONNECT_BACKOFF_MAX_SEC", "10.0"))

    # AsyncuaService: поддерживаем оба варианта (с конфигом и без)
    from app.daemon.asyncua_service import AsyncuaService

    svc = None
    try:
        # вариант с конфиг-объектом
        from app.daemon.asyncua_service import AsyncuaServiceConfig  # type: ignore

        svc = AsyncuaService(
            endpoint=endpoint_post,
            name="postamat",
            timeout_sec=timeout_sec,
            sub_interval_ms=sub_ms,
            reconnect_backoff_sec=backoff,
            reconnect_backoff_max_sec=backoff_max,

        )
    except Exception:
        # вариант без конфига (endpoint параметрами)
        svc = AsyncuaService(
            endpoint=endpoint_post,
            name="postamat",
            timeout_sec=timeout_sec,
            sub_interval_ms=sub_ms,
        )

    safety_ok = _env("OPCUA_NODE_SAFETY")
    trash = _env("OPCUA_NODE_TRASHCAN_PRESENT")
    if not safety_ok or not trash:
        raise RuntimeError("Missing OPCUA_NODE_SAFETY and/or OPCUA_NODE_TRASHCAN_PRESENT")

    nodes = OpcUaNodes(
        safety_ok=safety_ok,
        trashcan_present=trash,
        safety_status=_env("OPCUA_NODE_SAFETY_STATUS"),        
        # temperature sensors
        temperature_sensor_loading=_env("OPCUA_NODE_TEMPERATURE_SENSOR_LOADING"),
        temperature_sensor_im=_env("OPCUA_NODE_TEMPERATURE_SENSOR_IM"),        
        # doors cmd/status
        loading_door_cmd_fmt=_env("OPCUA_NODE_LOADING_DOOR_CMD_FMT"),
        unloading_door_cmd_fmt=_env("OPCUA_NODE_UNLOADING_DOOR_CMD_FMT"),
        loading_door_stat_fmt=_env("OPCUA_NODE_LOADING_DOOR_STAT_FMT"),
        unloading_door_stat_fmt=_env("OPCUA_NODE_UNLOADING_DOOR_STAT_FMT"),
        # column sensors
        loading_col_sensor_fmt=_env("OPCUA_NODE_LOADING_COL_SENSOR_FMT"),
        unloading_col_sensor_fmt=_env("OPCUA_NODE_UNLOADING_COL_SENSOR_FMT"),
        # stacklight: prefer int if present
        stacklight_color=_env("OPCUA_NODE_STACKLIGHT_STATE_CMD"),
        stacklight_sound=_env("OPCUA_NODE_SOUND_STATE_CMD"),
        #stacklight_color_status=_env("OPCUA_NODE_STACKLIGHT_STATE_STATUS"),
        #stacklight_sound_status=_env("OPCUA_NODE_SOUND_STATE_STATUS"),        
        #stack_sound=_env("OPCUA_NODE_SND"),
        #stack_green=_env("OPCUA_NODE_G"),
        #stack_yellow=_env("OPCUA_NODE_Y"),
        #stack_red=_env("OPCUA_NODE_R"),
    )

    loading_cells = _parse_int_list(_env("OPCUA_LOADING_CELLS"), default=range(1, 17))
    unloading_cells = _parse_int_list(_env("OPCUA_UNLOADING_CELLS"), default=range(1, 17))
    loading_cols = _parse_int_list(_env("OPCUA_LOADING_COLS"), default=(1, 2, 3, 4))
    unloading_cols = _parse_int_list(_env("OPCUA_UNLOADING_COLS"), default=(1, 2, 3, 4, 5))

    return OpcUaIO(
        endpoint=endpoint_post,
        nodes=nodes,
        svc=svc,
        read_fallback_sec=read_fallback_sec,
        loading_cells=loading_cells,
        unloading_cells=unloading_cells,
        loading_cols=loading_cols,
        unloading_cols=unloading_cols,
    )


def make_io(*, reject_bin_count_init: int = 0):
    """
    IO_MODE:
      - mock   : чистый MockIO (для тестов/разработки без железа)
      - hybrid : OpcUaIO + логический reject_bin_count
      - auto   : hybrid если есть OPCUA endpoint, иначе mock
    """
    mode = (_env("IO_MODE", "auto") or "auto").strip().lower()

    if mode == "auto":
        if _env("OPCUA_ENDPOINT_POSTAMAT") or _env("OPCUA_ENDPOINT"):
            mode = "hybrid"
        else:
            mode = "mock"

    if mode == "mock":
        # можно управлять начальными значениями через env
        safety_ok = _env_bool("MOCK_SAFETY_OK", True)
        trash_present = _env_bool("MOCK_TRASH_PRESENT", True)
        rb = _env_int("MOCK_REJECT_BIN_COUNT", int(reject_bin_count_init))

        io = MockIO(safety_ok=safety_ok, trashcan_present=trash_present)
        try:
            io.set_reject_bin_count(rb)
        except Exception:
            pass
        return io

    if mode == "hybrid":
        opcua = _make_postamat_opcua_io()
        return HybridIO(opcua=opcua, reject_bin_count_init=int(reject_bin_count_init))

    raise RuntimeError(f"Unknown IO_MODE={mode!r} (expected: mock|hybrid|auto)")
