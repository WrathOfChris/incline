import collections
from typing import Any


def flatten(val: Any, prefix: str = '', sep: str = '.') -> dict[str, Any]:
    """
    flatten a value
    """
    flat = dict()
    if isinstance(val, collections.abc.Mapping):
        for k, v in val.items():
            fk = f"{str(prefix)}{sep}{str(k)}"
            flat.update(flatten(v, prefix=fk, sep=sep))
    elif isinstance(val, str):
        flat[prefix] = val
    elif isinstance(val, collections.abc.Collection):
        for i, v in enumerate(val):
            fk = f"{str(prefix)}{sep}{str(i)}"
            flat.update(flatten(v, prefix=fk, sep=sep))
    else:
        flat[prefix] = val

    return flat
