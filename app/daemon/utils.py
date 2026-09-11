from __future__ import annotations

import re
from typing import Any

_SPEC_RE = re.compile(r"^(?P<base>.+?)(?:-(?P<spec>\d{1,3}))?$")

def parse_product_name_and_spec(product_name: str) -> tuple[str, int]:
    s = (product_name or "").strip()
    m = _SPEC_RE.match(s)
    if not m:
        return s, 0
    base = (m.group("base") or s).strip()
    spec_raw = m.group("spec")
    if spec_raw is None:
        return base, 0
    try:
        return base, int(spec_raw, 10)
    except Exception:
        return base, 0

###test
#pn_raw = "0401.17.02.023-01"
#pn_base, pn_spec = parse_product_name_and_spec(pn_raw)


_KEY_RE = re.compile(
    r"^(?:(?P<idx>\d+)-)?"      # idx — только цифры (возможно с ведущими нулями)
    r"(?P<base>.+?)"            # основная часть ключа
    r"(?P<kind>Val|Tol)$"       # суффикс Val/Tol
)

def _idx_to_int(idx: str | None) -> int:
    """Преобразует idx в число для сортировки (игнорируя ведущие нули)."""
    if not idx:
        return 10**9  # большие значения для ключей без idx
    s = str(idx).strip()
    if s.isdigit():
        return int(s)  # '01' → 1, '05' → 5, '41' → 41
    return 10**9  # резервное значение для некорректных idx (хотя regex это исключает)

def build_measurement_items(
    values: dict[str, Any],
    tols: dict[str, Any],
    *,
    decimals: int = 4,
) -> list[dict[str, Any]]:
    def _round_n(x: Any) -> Any:
        """Округление до заданного числа знаков после запятой."""
        if x is None:
            return None
        try:
            return round(float(x), int(decimals))
        except Exception:
            return x

    def _sort_key(k: str):
        """Ключ для сортировки: (idx, base, порядок Val/Tol)."""
        m = _KEY_RE.match(k or "")
        if not m:
            return (10**9, k, 2)  # некорректные ключи в конец
        idx = _idx_to_int(m.group("idx"))
        base = m.group("base")
        kind = m.group("kind")
        kind_order = 0 if kind == "Val" else 1  # Val перед Tol
        return (idx, base, kind_order)

    merged: Dict[str, Dict[str, Any]] = {}

    def _ensure(base: str, order: int) -> Dict[str, Any]:
        """Гарантирует существование записи для base, обновляя порядок сортировки."""
        it = merged.get(base)
        if not it:
            it = {"name": base, "value": None, "ok": None, "_order": order}
            merged[base] = it
        else:
            if order < int(it["_order"]):
                it["_order"] = order
        return it

    # Обработка значений (values)
    for k, v in sorted((values or {}).items(), key=lambda kv: _sort_key(kv[0])):
        m = _KEY_RE.match(k)
        if m:
            base = m.group("base")
            order = _idx_to_int(m.group("idx"))
            it = _ensure(base, order)
            if m.group("kind") == "Val":
                it["value"] = _round_n(v)
        else:
            # Ключ не соответствует шаблону - обрабатываем как отдельный элемент
            it = _ensure(k, 10**9)
            it["value"] = _round_n(v)

    # Обработка допусков (tols)
    for k, ok in sorted((tols or {}).items(), key=lambda kv: _sort_key(kv[0])):
        m = _KEY_RE.match(k)
        if m:
            base = m.group("base")
            order = _idx_to_int(m.group("idx"))
            it = _ensure(base, order)
            if m.group("kind") == "Tol":
                it["ok"] = bool(ok) if ok is not None else None
        else:
            # Ключ не соответствует шаблону
            it = _ensure(k, 10**9)
            it["ok"] = bool(ok) if ok is not None else None

    # Итоговая сортировка: по порядку, затем по имени
    merged_sorted = sorted(
        merged.values(),
        key=lambda x: (int(x["_order"]), str(x["name"]))
    )

    # Формируем итоговый список
    return [
        {"name": it["name"], "value": it["value"], "ok": it["ok"]}
        for it in merged_sorted
    ]