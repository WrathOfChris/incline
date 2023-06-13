from typing import Any
import collections.abc


def page(iterable: Any, count: int) -> collections.abc.Generator[list[Any], None, None]:
    """
    generate a list of lists
    ex: list(page(range(1,10), 3)) ->  [[1,2,3], [4,5,6], [7,8,9]]
    """
    pos = 0
    page = []
    for i in iterable:
        page.append(i)
        pos += 1
        if pos >= count:
            yield page
            page = []
            pos = 0
    if page:
        yield page
