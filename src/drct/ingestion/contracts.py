"""Versioned data contracts for the landing files, and schema diffing against them.

Landing files are read as all-string frames. ``diff_schema`` compares a frame to its contract:
a breaking diff (missing columns, unparseable values) quarantines the file, an additive one
(unknown columns) loads with a warning. ``canonicalise`` shapes a frame for the raw table.
"""

import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, get_args

import pandas as pd
import yaml

DType = Literal["string", "int", "decimal", "date", "timestamp", "bool"]
FileFormat = Literal["csv", "json", "parquet"]

DEFAULT_DIR = Path(__file__).resolve().parent / "contracts"
EXTRA_COLUMN = "_extra"

_INT = re.compile(r"[+-]?\d+")
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


@dataclass(frozen=True)
class Column:
    name: str
    dtype: DType
    nullable: bool = True
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class Contract:
    source: str
    version: int
    file_format: FileFormat
    columns: tuple[Column, ...]


@dataclass
class SchemaDiff:
    added: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    # Contract column -> first value that does not parse as its dtype.
    type_mismatches: dict[str, str] = field(default_factory=dict)
    # Name found in the file -> canonical contract name.
    aliased: dict[str, str] = field(default_factory=dict)

    @property
    def is_breaking(self) -> bool:
        return bool(self.missing or self.type_mismatches)

    @property
    def is_clean(self) -> bool:
        return not (self.added or self.missing or self.type_mismatches or self.aliased)


def load_contract(source: str, contracts_dir: Path = DEFAULT_DIR) -> Contract:
    raw = yaml.safe_load((contracts_dir / f"{source}.yml").read_text(encoding="utf-8"))
    columns = []
    for c in raw["columns"]:
        if c["dtype"] not in get_args(DType):
            raise ValueError(f"{source}.{c['name']}: unknown dtype {c['dtype']!r}")
        columns.append(
            Column(
                name=c["name"],
                dtype=c["dtype"],
                nullable=c.get("nullable", True),
                aliases=tuple(c.get("aliases") or ()),
            )
        )
    if raw["file_format"] not in get_args(FileFormat):
        raise ValueError(f"{source}: unknown file_format {raw['file_format']!r}")
    return Contract(
        source=raw["source"],
        version=int(raw["version"]),
        file_format=raw["file_format"],
        columns=tuple(columns),
    )


def _is_int(value: str) -> bool:
    return _INT.fullmatch(value) is not None


def _is_decimal(value: str) -> bool:
    try:
        return Decimal(value).is_finite() and "_" not in value
    except InvalidOperation:
        return False


def _is_date(value: str) -> bool:
    if not _ISO_DATE.fullmatch(value):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _is_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _is_bool(value: str) -> bool:
    return value.lower() in ("true", "false")


_PARSERS: dict[str, Callable[[str], bool]] = {
    "string": lambda value: True,
    "int": _is_int,
    "decimal": _is_decimal,
    "date": _is_date,
    "timestamp": _is_timestamp,
    "bool": _is_bool,
}


def _is_null(value: Any) -> bool:
    if isinstance(value, float):
        return math.isnan(value)
    return value is None or value is pd.NA or (isinstance(value, str) and value == "")


def _resolve_names(contract: Contract, df: pd.DataFrame) -> dict[str, str]:
    """Map each contract column to the name it has in ``df``; absent columns are left out.

    The canonical name wins over an alias, and aliases are tried in their declared order.
    """
    present = set(df.columns)
    found: dict[str, str] = {}
    for column in contract.columns:
        for candidate in (column.name, *column.aliases):
            if candidate in present:
                found[column.name] = candidate
                break
    return found


def diff_schema(contract: Contract, df: pd.DataFrame) -> SchemaDiff:
    """Compare ``df`` to ``contract``. Null values are skipped: dbt tests own row-level quality."""
    found = _resolve_names(contract, df)
    used = set(found.values())
    diff = SchemaDiff(
        added=sorted(str(c) for c in df.columns if c not in used),
        missing=sorted(c.name for c in contract.columns if c.name not in found),
        aliased={actual: name for name, actual in found.items() if actual != name},
    )
    for column in contract.columns:
        if column.name not in found or column.dtype == "string":
            continue
        parses = _PARSERS[column.dtype]
        for value in df[found[column.name]]:
            if not _is_null(value) and not parses(str(value)):
                diff.type_mismatches[column.name] = str(value)
                break
    diff.aliased = dict(sorted(diff.aliased.items()))
    return diff


def canonicalise(contract: Contract, df: pd.DataFrame) -> pd.DataFrame:
    """Contract columns in order (aliases renamed) plus ``_extra``; the input is not modified.

    ``_extra`` holds each row's unknown columns as a JSON object string, or None if the row has
    no non-null unknown values. A contract column absent from ``df`` comes out all-None.
    """
    found = _resolve_names(contract, df)
    used = set(found.values())
    out = pd.DataFrame(index=df.index)
    for column in contract.columns:
        if column.name in found:
            out[column.name] = df[found[column.name]]
        else:
            out[column.name] = pd.Series([None] * len(df), index=df.index, dtype=object)
    extras = [c for c in df.columns if c not in used]
    packed: list[str | None] = [None] * len(df)
    for i, row in enumerate(df[extras].itertuples(index=False, name=None)):
        values = {str(c): v for c, v in zip(extras, row, strict=True) if not _is_null(v)}
        if values:
            packed[i] = json.dumps(values, default=str)
    out[EXTRA_COLUMN] = pd.Series(packed, index=df.index, dtype=object)
    return out
