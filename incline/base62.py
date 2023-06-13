import string

BASE_LIST = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
BASE_DICT = dict((c, i) for i, c in enumerate(BASE_LIST))


def base_decode(string: str, reverse_base: dict[str, int] = BASE_DICT) -> int:
    string = str(string)
    length = len(reverse_base)
    ret = 0
    for i, c in enumerate(string[::-1]):
        ret += (length**i) * reverse_base[c]

    return ret


def base_encode(integer: int, base: str = BASE_LIST) -> str:
    if integer <= 0:
        return base[0]

    length = len(base)
    ret = ''
    while integer > 0:
        code = int(integer % length)
        ret = base[code] + ret
        integer -= code
        # floor division to remain int type
        integer //= length

    return ret
