from dataclasses import dataclass


class ProductRuleError(ValueError):
    """Ошибка проверки параметров детали/партии."""


@dataclass(frozen=True)
class ProductRule:
    product_code: str
    product_name: str
    product_spec: int
    global_max_tare_count: int
    current_max_tare_count: int
    max_product_count: int


# Полные обозначения перечислены явно: произвольные исполнения не допускаются.
_PRODUCT_VARIANTS: dict[str, tuple[str, int]] = {
    "312.229.001": ("312.229.001", 0),
    "312.229.001-01": ("312.229.001", 1),
    "312.229.001-02": ("312.229.001", 2),

    "312.229.002": ("312.229.002", 0),
    "312.229.002-01": ("312.229.002", 1),
    "312.229.002-02": ("312.229.002", 2),

    "440.00.026": ("440.00.026", 0),
    "440.00.026-01": ("440.00.026", 1),
    "440.00.026-02": ("440.00.026", 2),
    "440.00.026-03": ("440.00.026", 3),

    "440.00.111": ("440.00.111", 0),
    "440.00.111-02": ("440.00.111", 2),
    "440.00.111-03": ("440.00.111", 3),

    "0401.17.02.023": ("0401.17.02.023", 0),
    "0401.17.02.023-01": ("0401.17.02.023", 1),
    "0401.17.02.023-02": ("0401.17.02.023", 2),

    "0401.28.02.063": ("0401.28.02.063", 0),
    "0401.28.02.063-01": ("0401.28.02.063", 1),
}


# (базовое обозначение, UseAlternateWave, Layout) ->
# (GlobalMaxTareCount, CurrentMaxTareCount, max ProductCount)
_PRODUCT_RULES: dict[tuple[str, bool, int], tuple[int, int, int]] = {}


def _add_rules(
    product_name: str,
    use_alternate_wave: bool,
    global_max_tare_count: int,
    current_max_tare_count: int,
    max_counts: tuple[int, int, int, int],
) -> None:
    for layout, max_product_count in enumerate(max_counts):
        _PRODUCT_RULES[(product_name, use_alternate_wave, layout)] = (
            int(global_max_tare_count),
            int(current_max_tare_count),
            int(max_product_count),
        )


# UseAlternateWave = False
_add_rules("312.229.001", False, 77, 77, (231, 231, 231, 231))
_add_rules("312.229.002", False, 147, 126, (396, 396, 378, 378))
_add_rules("440.00.026", False, 168, 147, (440, 440, 440, 440))
_add_rules("440.00.111", False, 231, 231, (660, 660, 660, 660))
_add_rules("0401.17.02.023", False, 105, 84, (264, 264, 252, 252))
_add_rules("0401.28.02.063", False, 126, 126, (378, 378, 378, 378))

# UseAlternateWave = True
_add_rules("312.229.001", True, 77, 77, (231, 231, 231, 231))
_add_rules("312.229.002", True, 147, 126, (396, 396, 372, 372))
_add_rules("440.00.026", True, 168, 147, (440, 440, 434, 434))
_add_rules("440.00.111", True, 231, 231, (660, 660, 660, 660))
_add_rules("0401.17.02.023", True, 105, 84, (264, 264, 248, 248))
_add_rules("0401.28.02.063", True, 126, 126, (372, 372, 372, 372))


_SPECIAL_UNLOADING_PRODUCT_CODES = frozenset({
    "312.229.001",
    "312.229.001-01",
    "312.229.001-02",
})

_LAYOUT_NAMES = {
    0: "А",
    1: "Б",
    2: "В",
    3: "Г",
}


def normalize_product_code(value: object) -> str:
    return str(value or "").strip()


def is_supported_product_code(value: object) -> bool:
    return normalize_product_code(value) in _PRODUCT_VARIANTS


def uses_special_unloading_cell(value: object) -> bool:
    return normalize_product_code(value) in _SPECIAL_UNLOADING_PRODUCT_CODES


def product_code_from_name_and_spec(
    product_name: object,
    product_spec: object,
) -> str:
    """Восстанавливает полное обозначение для RTK-команд без batch-контекста."""
    name = normalize_product_code(product_name)

    try:
        spec = int(product_spec or 0)
    except (TypeError, ValueError):
        spec = 0

    if name in _PRODUCT_VARIANTS:
        stored_name, stored_spec = _PRODUCT_VARIANTS[name]
        if stored_spec == spec:
            return name
        if stored_spec == 0 and spec > 0:
            candidate = f"{stored_name}-{spec:02d}"
            if candidate in _PRODUCT_VARIANTS:
                return candidate

    if spec > 0:
        candidate = f"{name}-{spec:02d}"
        if candidate in _PRODUCT_VARIANTS:
            return candidate

    return name


def resolve_product_rule(
    product_code: object,
    layout: object,
    use_alternate_wave: object,
    product_count: object | None = None,
) -> ProductRule:
    code = normalize_product_code(product_code)

    variant = _PRODUCT_VARIANTS.get(code)
    if variant is None:
        shown_code = code or "-"
        raise ProductRuleError(
            f"Деталь `{shown_code}` не поддерживается системой."
        )

    if isinstance(layout, bool):
        raise ProductRuleError("Раскладка должна иметь значение 0, 1, 2 или 3.")

    try:
        layout_i = int(layout)
    except (TypeError, ValueError):
        raise ProductRuleError(
            "Раскладка должна иметь значение 0, 1, 2 или 3."
        )

    if layout_i not in _LAYOUT_NAMES:
        raise ProductRuleError(
            "Раскладка должна иметь значение 0, 1, 2 или 3."
        )

    if not isinstance(use_alternate_wave, bool):
        raise ProductRuleError("Параметр «Противоволна» должен иметь логическое значение.")

    product_name, product_spec = variant
    values = _PRODUCT_RULES.get(
        (product_name, use_alternate_wave, layout_i)
    )

    if values is None:
        raise ProductRuleError(
            f"Для детали `{code}` не настроены параметры запуска РТК."
        )

    global_max, current_max, max_product_count = values

    if product_count is not None:
        if isinstance(product_count, bool):
            raise ProductRuleError(
                "Количество должно быть целым числом больше нуля."
            )

        try:
            product_count_i = int(product_count)
        except (TypeError, ValueError):
            raise ProductRuleError(
                "Количество должно быть целым числом больше нуля."
            )

        if product_count_i <= 0:
            raise ProductRuleError(
                "Количество должно быть целым числом больше нуля."
            )

        if product_count_i > max_product_count:
            wave_text = (
                "с включённой Противоволной"
                if use_alternate_wave
                else "с выключенной Противоволной"
            )
            raise ProductRuleError(
                f"Для детали `{code}`, раскладки "
                f"{_LAYOUT_NAMES[layout_i]} и {wave_text} "
                f"допустимо не более {max_product_count} деталей. "
                f"Введено: {product_count_i}."
            )

    return ProductRule(
        product_code=code,
        product_name=product_name,
        product_spec=int(product_spec),
        global_max_tare_count=int(global_max),
        current_max_tare_count=int(current_max),
        max_product_count=int(max_product_count),
    )
