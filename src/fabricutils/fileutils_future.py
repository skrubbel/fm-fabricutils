from __future__ import annotations
from typing import Any, Dict, Mapping, Optional
from decimal import Decimal, ROUND_HALF_EVEN

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.types as pat


# --------------------------
# DTYPE SELECTION (non-Arrow backend only)
# --------------------------
def _arrow_type_to_pandas_nullable_dtype(t: pa.DataType) -> Any:
    """Pandas dtypes for when dtype_backend != 'pyarrow' (no ArrowDtype here)."""
    if pat.is_int8(t):
        return pd.Int8Dtype()
    if pat.is_int16(t):
        return pd.Int16Dtype()
    if pat.is_int32(t):
        return pd.Int32Dtype()
    if pat.is_int64(t):
        return pd.Int64Dtype()
    if pat.is_uint8(t):
        return pd.UInt8Dtype()
    if pat.is_uint16(t):
        return pd.UInt16Dtype()
    if pat.is_uint32(t):
        return pd.UInt32Dtype()
    if pat.is_uint64(t):
        return pd.UInt64Dtype()

    # pandas has Float32/Float64 nullable dtypes
    if pat.is_float16(t):
        return pd.Float32Dtype()
    if pat.is_float32(t):
        return pd.Float32Dtype()
    if pat.is_float64(t):
        return pd.Float64Dtype()

    if pat.is_boolean(t):
        return pd.BooleanDtype()

    if pat.is_string(t) or pat.is_large_string(t):
        return pd.StringDtype()
    if pat.is_binary(t) or pat.is_large_binary(t):
        return "object"

    if pat.is_date32(t) or pat.is_date64(t) or pat.is_timestamp(t):
        return "datetime64[ns]"

    return "object"


def _build_pandas_dtype_map_for_non_arrow_backend(
    schema: pa.Schema, column_map: Optional[Mapping[str, str]]
) -> Dict[str, Any]:
    """Original Excel names -> pandas (nullable) dtypes (no ArrowDtype)."""
    dtype_map: Dict[str, Any] = {}
    if not column_map:
        for f in schema:
            dtype_map[f.name] = _arrow_type_to_pandas_nullable_dtype(f.type)
        return dtype_map

    name_to_field = {f.name: f for f in schema}  # alias name -> field
    for src_original, tgt_alias in column_map.items():
        f = name_to_field.get(tgt_alias)
        if f is None:
            continue
        dtype_map[src_original] = _arrow_type_to_pandas_nullable_dtype(f.type)
    return dtype_map


# --------------------------
# NORMALIZATION NICETIES
# --------------------------
def _normalize_dataframe_after_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make Excel output Arrow-friendly:
      - Convert empty strings to <NA> across *all* columns
      - Convert 'object' columns to pandas StringDtype (nullable)
      - Normalize string columns (strip) without touching NA
    """
    # 1) Turn empty strings into <NA> for everything
    df = df.replace("", pd.NA)

    # 2) Upgrade generic 'object' columns to nullable strings
    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols):
        df = df.astype({c: "string" for c in obj_cols})

    # 3) Clean strings (strip); keep NA as NA
    str_cols = df.select_dtypes(include=["string"]).columns
    for c in str_cols:
        df[c] = df[c].str.strip()

    return df


def _coerce_booleans_textual(s: pd.Series) -> pd.Series:
    """Interpret common textual booleans before Arrow casting."""
    if pd.api.types.is_string_dtype(s.dtype):
        return s.str.lower().map({"true": True, "false": False, "yes": True, "no": False, "1": True, "0": False})
    return s


def _maybe_localize_and_convert_timestamp_series(
    s: pd.Series,
    arrow_ts: pa.TimestampType,
    local_tz: Optional[str] = None,
) -> pd.Series:
    """
    If Arrow schema requires timezone, ensure the pandas series matches.
    Behavior:
      - If Arrow has tz=None: ensure tz-naive pandas (convert -> naive)
      - If Arrow has tz='XYZ':
          * strings -> parse then localize/convert
          * naive -> localize to `local_tz` (or Arrow tz if local_tz is None)
          * tz-aware -> convert to Arrow tz
    """
    target_tz = arrow_ts.tz  # may be None
    # If it's strings or anything non-datetime, try to parse
    if not pd.api.types.is_datetime64_any_dtype(s.dtype):
        s = pd.to_datetime(s, errors="coerce", utc=False)

    if target_tz is None:
        # Ensure tz-naive
        if pd.api.types.is_datetime64tz_dtype(s.dtype):
            s = s.dt.tz_convert("UTC").dt.tz_localize(None)  # make naive in UTC wall-clock
        return s

    # Target has a timezone
    desired = target_tz
    if not pd.api.types.is_datetime64tz_dtype(s.dtype):
        # naive datetimes -> localize first
        # if user provided a local timezone, use it; else assume same as Arrow tz
        tz_to_use = local_tz or desired
        s = s.dt.tz_localize(tz_to_use, nonexistent="shift_forward", ambiguous="NaT")
    # Convert to the Arrow timezone
    s = s.dt.tz_convert(desired)
    return s


def _coerce_decimal_series_to_schema(
    s: pd.Series,
    dec_type: pa.Decimal128Type | pa.Decimal256Type,
) -> pd.Series:
    """
    Robust decimal handling:
      - If floats, convert to Decimal via str, then quantize to Arrow scale
      - If strings, convert to Decimal; empty/NA preserved
      - If already Decimal, quantize to target scale
    Result stays as Python Decimal objects; Arrow can ingest these for decimal types.
    """
    if isinstance(dec_type, pa.Decimal128Type) or isinstance(dec_type, pa.Decimal256Type):
        scale = dec_type.scale
    else:
        # should never happen but be defensive
        scale = 0

    quant = Decimal(1).scaleb(-scale)  # 10^-scale

    def _to_quantized_decimal(x: Any):
        if x is None or (isinstance(x, float) and np.isnan(x)) or (isinstance(x, str) and x == "") or pd.isna(x):
            return None
        # Use string conversion to avoid binary float artifacts
        return Decimal(str(x)).quantize(quant, rounding=ROUND_HALF_EVEN)

    # Floats or strings or objects all mapped to Decimal or None
    return s.map(_to_quantized_decimal)


# --------------------------
# ARROW TABLE CONSTRUCTION
# --------------------------
def _to_arrow_table_with_schema(
    df: pd.DataFrame,
    schema: pa.Schema,
    local_time_zone: Optional[str] = None,
) -> pa.Table:
    """
    Build an Arrow table using the *target* schema column-by-column
    with all niceties applied.
    """
    # Ensure all target columns exist
    for f in schema:
        if f.name not in df.columns:
            df[f.name] = pd.NA

    # Select & order
    df = df[[f.name for f in schema]]

    arrays = []
    for f in schema:
        s = df[f.name]

        # 1) textual booleans -> real booleans
        if pat.is_boolean(f.type):
            s = _coerce_booleans_textual(s)

        # 2) timestamps with timezone handling
        if pat.is_timestamp(f.type):
            s = _maybe_localize_and_convert_timestamp_series(s, f.type, local_tz=local_time_zone)

        # 3) decimals with quantization to schema's scale
        if pat.is_decimal(f.type):
            s = _coerce_decimal_series_to_schema(s, f.type)

        # 4) Let Arrow do the final cast; from_pandas=True respects NA
        arr = pa.array(s, type=f.type, from_pandas=True, safe=False)
        arrays.append(arr)

    return pa.Table.from_arrays(arrays, schema=schema)


# --------------------------
# PUBLIC API
# --------------------------
def excel_sheet_to_arrow_table(
    abfss_path: str,
    sheet_name: str,
    schema: pa.Schema,
    column_map: Optional[Mapping[str, str]] = None,
    *,
    local_time_zone: Optional[str] = None,  # e.g. "Europe/Copenhagen"
) -> pa.Table:
    """
    Robust Excel -> Arrow with niceties:
      - empty strings -> <NA> (all columns)
      - object -> StringDtype
      - boolean text normalization
      - timestamp tz localization/conversion per schema
      - decimal quantization per schema scale
    """
    # 1) Parse Excel without dtype= (most robust)
    df = pd.read_excel(abfss_path, sheet_name=sheet_name)

    # 2) Keep/rename
    if column_map:
        keep = [c for c in df.columns if c in column_map]
        df = df[keep].rename(columns=column_map)

    # 3) Normalize
    df = _normalize_dataframe_after_excel(df)

    # 4) Build Arrow respecting the target schema and niceties
    return _to_arrow_table_with_schema(df, schema, local_time_zone=local_time_zone)


def excel_sheet_to_pandas_dataframe(
    abfss_path: str,
    sheet_name: str,
    schema: pa.Schema,
    column_map: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    """
    Excel -> pandas, friendly to both dtype backends.
    (Arrow niceties are applied later when converting to Arrow.)
    """
    use_arrow_backend = pd.get_option("mode.dtype_backend") == "pyarrow"

    if use_arrow_backend:
        df = pd.read_excel(abfss_path, sheet_name=sheet_name)
    else:
        dtype_map = _build_pandas_dtype_map_for_non_arrow_backend(schema, column_map)
        df = pd.read_excel(abfss_path, sheet_name=sheet_name, dtype=dtype_map or None)

    if column_map:
        keep = [c for c in df.columns if c in column_map]
        df = df[keep].rename(columns=column_map)

    df = _normalize_dataframe_after_excel(df)
    return df
