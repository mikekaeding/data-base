from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

class Dataset:
    def to_table(self, columns: Sequence[str] | None = ...) -> pa.Table: ...

def dataset(source: str, format: str = ...) -> Dataset: ...
