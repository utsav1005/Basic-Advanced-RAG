"""batched — split a list into fixed-size groups so a 300-chunk document is
a handful of encode calls, not one giant tensor (or 300 tiny ones)."""

from collections.abc import Iterator
from itertools import islice
from typing import TypeVar

T = TypeVar("T")


def batched(items: list[T], size: int) -> Iterator[list[T]]:
    it = iter(items)
    while group := list(islice(it, size)):
        yield group


if __name__ == "__main__":
    assert list(batched([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
    assert list(batched([], 2)) == []
    print("batch_processor: OK")
