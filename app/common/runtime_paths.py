from __future__ import annotations

import os
from pathlib import Path


# app/common/runtime_paths.py -> app/common -> app -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _project_relative_path(raw_path: str) -> Path:
    """
    Абсолютный путь оставляет без изменения.
    Относительный путь вычисляет от корня проекта, а не от cwd процесса.
    """
    path = Path(str(raw_path).strip()).expanduser()

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path.resolve(strict=False)


def get_runtime_dir() -> Path:
    """
    Общая runtime-папка daemon и API.

    APP_RUNTIME_DIR может быть абсолютным либо относительным корню проекта.
    По умолчанию используется <project>/runtime.
    """
    configured = str(os.getenv("APP_RUNTIME_DIR", "") or "").strip()

    if configured:
        return _project_relative_path(configured)

    return (PROJECT_ROOT / "runtime").resolve(strict=False)


def get_temperature_snapshot_path() -> Path:
    """
    Путь к оперативному snapshot температуры.

    TEMPERATURE_SNAPSHOT_FILE имеет приоритет над APP_RUNTIME_DIR.
    Относительное значение считается от корня проекта.
    """
    configured = str(
        os.getenv("TEMPERATURE_SNAPSHOT_FILE", "")
        or ""
    ).strip()

    if configured:
        return _project_relative_path(configured)

    return get_runtime_dir() / "temperature_snapshot.json"
