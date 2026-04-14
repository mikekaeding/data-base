from __future__ import annotations

from os import PathLike

import pyarrow as pa

class ColumnChunkMetaData:
    total_compressed_size: int
    total_uncompressed_size: int

class RowGroupMetaData:
    num_columns: int

    def column(self, index: int) -> ColumnChunkMetaData: ...

class FileMetaData:
    num_rows: int
    num_row_groups: int

    def row_group(self, index: int) -> RowGroupMetaData: ...

class ParquetFile:
    metadata: FileMetaData
    schema_arrow: pa.Schema

    def __init__(self, source: str | PathLike[str]) -> None: ...

def write_table(table: pa.Table, where: str | PathLike[str]) -> None: ...
