from typing import Any, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.types as pat


def _arrow_type_to_pandas_dtype(t: pa.DataType) -> Any:
    """Return a pandas dtype corresponding to the arrow type

    Args:
        t (pa.DataType): The PyArrow DataType to convert.

    Returns:
        Any: Pandas.api DType or ArrowDtype
    """
    if pa.types.is_int8(t):
        return pd.Int8Dtype()
    if pa.types.is_int16(t):
        return pd.Int16Dtype()
    if pa.types.is_int32(t):
        return pd.Int32Dtype()
    if pa.types.is_int64(t):
        return pd.Int64Dtype()
    if pa.types.is_uint8(t):
        return pd.UInt8Dtype()
    if pa.types.is_uint16(t):
        return pd.UInt16Dtype()
    if pa.types.is_uint32(t):
        return pd.UInt32Dtype()
    if pa.types.is_uint64(t):
        return pd.UInt64Dtype()
    if pa.types.is_float32(t):
        return "float32"
    if pa.types.is_float64(t):
        return "float64"
    if pa.types.is_boolean(t):
        return pd.BooleanDtype()
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return pd.StringDtype()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "object"
    if pa.types.is_date32(t) or pa.types.is_date64(t) or pat.is_timestamp(t):
        return "datetime64[ns]"
    return pd.ArrowDtype(t)


def _build_pandas_dtype_map(schema: pa.Schema, column_map: Dict[str, str]) -> Dict[str, str]:
    """
    Build a pandas dtype map from ORIGINAL Excel column names.
    Columns not present in the sheet are ignored by pandas.
    """
    dtype_map: Dict[str, str] = {}
    name_to_field = {f.name: f for f in schema}  # schema names are your ALIASES
    # We don't know the original names in the schema, so we only
    # apply dtypes for columns whose names exactly match the schema names.
    # If you're using column_map (orig->alias), dtypes are applied post-rename.
    for source, target in column_map.items():
        f = name_to_field.get(target)

        if f is None:
            continue

        pd_dtype = _arrow_type_to_pandas_dtype(f.type)

        if pd_dtype is not None:
            dtype_map[source] = pd_dtype

    return dtype_map


def excel_sheet_to_arrow_table(
    abfss_path: str, sheet_name: str, schema: pa.Schema, column_map: Dict[str, str] | None = None
) -> pa.Table:
    dtype_map = _build_pandas_dtype_map(schema=schema, column_map=column_map)

    excel_df = pd.read_excel(abfss_path, sheet_name=sheet_name, dtype=dtype_map if dtype_map else None)

    if column_map:
        keep = [c for c in excel_df.columns if c in column_map]
        excel_df = excel_df[keep].rename(columns=column_map)

    tbl = pa.Table.from_pandas(excel_df, preserve_index=False)

    # Ensure columns exist & order matches schema; add missing as nulls
    for f in schema:
        if f.name not in tbl.column_names:
            tbl = tbl.append_column(f.name, pa.nulls(len(tbl)))
    # Reorder
    tbl = tbl.select([f.name for f in schema])

    # Final cast to the exact target schema.
    #    Use safe=False so Arrow will perform best-effort coercions (e.g., strings -> ints).
    tbl = tbl.cast(schema, safe=False)

    return tbl


def excel_sheet_to_pandas_dataframe(
    abfss_path: str, sheet_name: str, schema: pa.Schema, column_map: Dict[str, str] | None = None
) -> pd.DataFrame:
    dtype_map = _build_pandas_dtype_map(schema=schema, column_map=column_map)

    excel_df = pd.read_excel(abfss_path, sheet_name=sheet_name, dtype=dtype_map if dtype_map else None)

    # excel_df = pd.read_excel(abfss_path, sheet_name=sheet_name)

    # 1) Cast every object column to pandas' StringDtype (nullable string)
    obj_cols = excel_df.select_dtypes(include=["object"]).columns
    excel_df = excel_df.astype({c: "string" for c in obj_cols})

    if column_map:
        keep = [c for c in excel_df.columns if c in column_map]
        excel_df = excel_df[keep].rename(columns=column_map)

    return excel_df
