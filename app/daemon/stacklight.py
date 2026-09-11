def encode_stacklight_bitmask(*, sound: bool, green: bool, yellow: bool, red: bool) -> int:
    # bit0=G, bit1=Y, bit2=R, bit3=SOUND  -> 0..15
    return (1 if green else 0) | (2 if yellow else 0) | (4 if red else 0) | (8 if sound else 0)


def decode_stacklight_bitmask(state: int) -> tuple[bool, bool, bool, bool]:
    state = int(state) & 0x0F
    green = bool(state & 1)
    yellow = bool(state & 2)
    red = bool(state & 4)
    sound = bool(state & 8)
    return sound, green, yellow, red
