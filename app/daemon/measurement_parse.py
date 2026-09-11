from __future__ import annotations

import json
from typing import Any


def _norm_key(k: str) -> tuple[str, str | None]:
    """
    '3-D2maxVal' -> ('D2max', 'val')
    '9-L2maxTol' -> ('L2max', 'tol')
    """
    k = str(k).strip().strip('"')
    if "-" in k:
        _, k = k.split("-", 1)
    if k.endswith("Val"):
        return k[:-3], "val"
    if k.endswith("Tol"):
        return k[:-3], "tol"
    return k, None


def parse_csv_measurement_line(line: str) -> tuple[dict[str, float], dict[str, bool]]:
    """
    Формат из файла:
      "<json_values>";"<json_tols>"
    где json внутри с удвоенными кавычками "".
    """
    s = line.strip()
    if '";"' not in s:
        raise ValueError("expected csv form '\"{...}\";\"{...}\"'")

    a, b = s.split('";"', 1)
    if a.startswith('"'):
        a = a[1:]
    if b.endswith('"'):
        b = b[:-1]

    a = a.replace('""', '"')
    b = b.replace('""', '"')

    values_obj = json.loads(a)
    tols_obj = json.loads(b)

    values: dict[str, float] = {}
    tols: dict[str, bool] = {}

    for k, v in values_obj.items():
        name, kind = _norm_key(k)
        if kind == "val" and v is not None:
            values[name] = float(v)

    for k, v in tols_obj.items():
        name, kind = _norm_key(k)
        if kind == "tol" and v is not None:
            tols[name] = bool(v)

    return values, tols


def parse_measurement_payload(payload: dict[str, Any]) -> tuple[dict[str, float], dict[str, bool]]:
    """
    Поддерживаем:
    - payload['csv_line'] / ['raw'] = csv строка "<json>";"<json>"
    - payload['values'] + payload['tols'] dict
    - payload['params'] = [{name,value,ok},...]
    """
    if isinstance(payload.get("params"), list):
        values: dict[str, float] = {}
        tols: dict[str, bool] = {}
        for p in payload["params"]:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            if not name:
                continue
            if p.get("value") is not None:
                values[name] = float(p["value"])
            if p.get("ok") is not None:
                tols[name] = bool(p["ok"])
        return values, tols

    if isinstance(payload.get("values"), dict) and isinstance(payload.get("tols"), dict):
        values: dict[str, float] = {}
        tols: dict[str, bool] = {}
        for k, v in payload["values"].items():
            name, kind = _norm_key(k)
            if kind == "val" and v is not None:
                values[name] = float(v)
        for k, v in payload["tols"].items():
            name, kind = _norm_key(k)
            if kind == "tol" and v is not None:
                tols[name] = bool(v)
        return values, tols

    raw = payload.get("csv_line") or payload.get("raw")
    if isinstance(raw, str):
        return parse_csv_measurement_line(raw)

    raise ValueError("unsupported measurement payload")
