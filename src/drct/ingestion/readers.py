"""Read landing files into all-string frames: every cell is a ``str`` or ``None``."""

import json
import math
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

_FILE_DATE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.[A-Za-z0-9]+$")


def parse_business_date(path: Path) -> date:
    """The business date in a ``<source>_<YYYY-MM-DD>.<ext>`` file name."""
    match = _FILE_DATE.search(path.name)
    if match is None:
        raise ValueError(f"no business date in file name {path.name!r}")
    return date.fromisoformat(match.group(1))


def _cell(value: Any, blank_is_null: bool) -> str | None:
    """NaN, NaT, NA and None become None; everything else becomes its string form."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value)
    return None if blank_is_null and text == "" else text


def _as_strings(df: pd.DataFrame, blank_is_null: bool = False) -> pd.DataFrame:
    """Rebuild ``df`` with object columns of str/None (no dtype can turn None back into NaN)."""
    return pd.DataFrame(
        {
            column: pd.Series(
                [_cell(v, blank_is_null) for v in df[column]], index=df.index, dtype=object
            )
            for column in df.columns
        },
        index=df.index,
        columns=df.columns,
    )


def _read_csv(path: Path) -> pd.DataFrame:
    # keep_default_na=False keeps "NA"/"null" as text and leaves "" for blanks, which are nulls.
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    return _as_strings(df, blank_is_null=True)


def _json_cell(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _read_json(path: Path) -> pd.DataFrame:
    # No dtype inference anywhere (pd.DataFrame(records) would turn 3 into 3.0 beside a null and
    # 1.10 into 1.1): numbers are kept as their source text, booleans as true/false.
    records = json.loads(path.read_text(encoding="utf-8"), parse_int=str, parse_float=str)
    columns = list(dict.fromkeys(key for record in records for key in record))
    return pd.DataFrame(
        {
            column: pd.Series([_json_cell(record.get(column)) for record in records], dtype=object)
            for column in columns
        },
        columns=columns,
    )


def _read_parquet(path: Path) -> pd.DataFrame:
    return _as_strings(pd.read_parquet(path))


_READERS = {".csv": _read_csv, ".json": _read_json, ".parquet": _read_parquet}


def read_landing_file(path: Path) -> pd.DataFrame:
    """Read a landing file, dispatching on its extension.

    Every column is object dtype holding only ``str`` or ``None``, so the contract type-checker
    never meets a NaN, NaT or number.
    """
    try:
        reader = _READERS[path.suffix.lower()]
    except KeyError:
        raise ValueError(f"unsupported landing file type {path.suffix!r}: {path.name}") from None
    return reader(path)
