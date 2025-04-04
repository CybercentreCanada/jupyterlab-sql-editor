import datetime
import itertools
import math
import re
import string
from decimal import Decimal
from html import escape
from html import escape as html_escape
from os import environ
from typing import Any, Callable, List, Optional, Union, cast

import pandas as pd
import pyspark.sql.types as pt
from ipyaggrid import Grid
from ipydatagrid import DataGrid, TextRenderer
from numpy import datetime64, ndarray
from pandas.core.series import Series as PandasSeriesLike
from pandas.errors import OutOfBoundsDatetime
from pyspark.errors.exceptions.base import PySparkException
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DataType,
    DayTimeIntervalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntegralType,
    LongType,
    MapType,
    Row,
    ShortType,
    StructType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
    _create_row,
)
from trino.types import NamedRowTuple

DEFAULT_COLUMN_DEF = {"editable": False, "filter": True, "resizable": True, "sortable": True}

PRINTABLE = string.ascii_letters + string.digits + string.punctuation + " "

replchars = re.compile("([^" + re.escape(PRINTABLE) + "])")

JS_MAX_SAFE_INTEGER = 9007199254740991

JS_MIN_SAFE_INTEGER = -9007199254740991


def render_text(rows, columns, truncate):
    string_builder = ""
    num_cols = len(columns)
    # We set a minimum column width at '3'
    minimum_col_width = 3

    # Initialise the width of each column to a minimum value
    col_widths = [minimum_col_width for i in range(num_cols)]

    # Truncate results
    truncated_rows = []
    for row in rows:
        new_row = []
        for i, cell in enumerate(row):
            cell_value = format_value(str(cell), False, truncate)
            new_row.append(cell_value)
        truncated_rows.append(new_row)

    # Compute the width of each column
    for i, column in enumerate(columns):
        col_widths[i] = max(col_widths[i], len(column))
    for row in truncated_rows:
        for i, cell in enumerate(row):
            # 3 for the 3 dots after truncating
            col_widths[i] = max(col_widths[i], len(str(cell)))

    padded_columns = []
    for i, column in enumerate(columns):
        new_name = column.ljust(col_widths[i], " ")
        padded_columns.append(new_name)

    padded_rows = []
    for row in truncated_rows:
        new_row = []
        for i, cell in enumerate(row):
            new_val = cell.ljust(col_widths[i], " ")
            new_row.append(new_val)
        padded_rows.append(new_row)

    # Create SeparateLine
    sep = "+"
    for width in col_widths:
        for i in range(width):
            sep += "-"
        sep += "+"
    sep += "\n"

    string_builder = sep
    string_builder += "|"
    for column in padded_columns:
        string_builder += column + "|"
    string_builder += "\n"

    # data
    string_builder += sep
    for row in padded_rows:
        string_builder += "|"
        for cell in row:
            string_builder += cell + "|"
        string_builder += "\n"
    string_builder += sep
    return string_builder


def recursive_escape(input):
    # check whether it's a dict, list, tuple, or scalar
    if isinstance(input, dict):
        items = input.items()
    elif isinstance(input, (list, tuple)):
        items = enumerate(input)
    else:
        # just a value, split and return
        return format_value(str(input))

    # now call ourself for every value and replace in the input
    for key, value in items:
        input[key] = recursive_escape(value)
    return input


def make_tag(tag_name, show_nonprinting, body="", **kwargs):
    body = str(body)
    if show_nonprinting:
        body = format_value(escape(body))
    attributes = " ".join(map(lambda x: '%s="%s"' % x, kwargs.items()))
    if attributes:
        return f"<{tag_name} {attributes}>{body}</{tag_name}>"
    else:
        return f"<{tag_name}>{body}</{tag_name}>"


# PySpark 3.5.3 copy
class UnsupportedOperationException(PySparkException):
    """
    Unsupported operation exception thrown from Spark with an error class.
    """


def _get_local_timezone() -> str:
    """Get local timezone using pytz with environment variable, or dateutil.

    If there is a 'TZ' environment variable, pass it to pandas to use pytz and use it as timezone
    string, otherwise use the special word 'dateutil/:' which means that pandas uses dateutil and
    it reads system configuration to know the system local timezone.

    See also:
    - https://github.com/pandas-dev/pandas/blob/0.19.x/pandas/tslib.pyx#L1753
    - https://github.com/dateutil/dateutil/blob/2.6.1/dateutil/tz/tz.py#L1338
    """
    import os

    return os.environ.get("TZ", "dateutil/:")


def _to_corrected_pandas_type(dt: DataType) -> Optional[Any]:
    """
    When converting Spark SQL records to Pandas `pandas.DataFrame`, the inferred data type
    may be wrong. This method gets the corrected data type for Pandas if that type may be
    inferred incorrectly.
    """
    import numpy as np

    if type(dt) == ByteType:
        return np.int8
    elif type(dt) == ShortType:
        return np.int16
    elif type(dt) == IntegerType:
        return np.int32
    elif type(dt) == LongType:
        return np.int64
    elif type(dt) == FloatType:
        return np.float32
    elif type(dt) == DoubleType:
        return np.float64
    elif type(dt) == BooleanType:
        return bool
    elif type(dt) == TimestampType:
        return np.dtype("datetime64[ns]")
    elif type(dt) == TimestampNTZType:
        return np.dtype("datetime64[ns]")
    elif type(dt) == DayTimeIntervalType:
        return np.dtype("timedelta64[ns]")
    else:
        return None


def _check_series_convert_timestamps_localize(
    s: "PandasSeriesLike", from_timezone: Optional[str], to_timezone: Optional[str]
) -> "PandasSeriesLike":
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    Parameters
    ----------
    s : pandas.Series
    from_timezone : str
        the timezone to convert from. if None then use local timezone
    to_timezone : str
        the timezone to convert to. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` where if it is a timestamp, has been converted to tz-naive
    """
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()

    import pandas as pd
    from pandas.api.types import (  # type: ignore[attr-defined]
        is_datetime64_dtype,
        is_datetime64tz_dtype,
    )

    from_tz = from_timezone or _get_local_timezone()
    to_tz = to_timezone or _get_local_timezone()
    # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
    if is_datetime64tz_dtype(s.dtype):
        return s.dt.tz_convert(to_tz).dt.tz_localize(None)
    elif is_datetime64_dtype(s.dtype) and from_tz != to_tz:
        # `s.dt.tz_localize('tzlocal()')` doesn't work properly when including NaT.
        return cast(
            "PandasSeriesLike",
            s.apply(
                lambda ts: (
                    ts.tz_localize(from_tz, ambiguous=False).tz_convert(to_tz).tz_localize(None)
                    if ts is not pd.NaT
                    else pd.NaT
                )
            ),
        )
    else:
        return s


def _check_series_convert_timestamps_local_tz(s: "PandasSeriesLike", timezone: str) -> "PandasSeriesLike":
    """
    Convert timestamp to timezone-naive in the specified timezone or local timezone

    Parameters
    ----------
    s : pandas.Series
    timezone : str
        the timezone to convert to. if None then use local timezone

    Returns
    -------
    pandas.Series
        `pandas.Series` where if it is a timestamp, has been converted to tz-naive
    """
    return _check_series_convert_timestamps_localize(s, None, timezone)


def _dedup_names(names: List[str]) -> List[str]:
    if len(set(names)) == len(names):
        return names
    else:

        def _gen_dedup(_name: str) -> Callable[[], str]:
            _i = itertools.count()
            return lambda: f"{_name}_{next(_i)}"

        def _gen_identity(_name: str) -> Callable[[], str]:
            return lambda: _name

        gen_new_name = {
            name: _gen_dedup(name) if len(list(group)) > 1 else _gen_identity(name)
            for name, group in itertools.groupby(sorted(names))
        }
        return [gen_new_name[name]() for name in names]


def _create_converter_to_pandas(
    data_type: DataType,
    nullable: bool = True,
    *,
    timezone: Optional[str] = None,
    struct_in_pandas: Optional[str] = None,
    error_on_duplicated_field_names: bool = True,
    timestamp_utc_localized: bool = True,
    ndarray_as_list: bool = False,
) -> Callable[["pd.Series"], "pd.Series"]:
    """
    Create a converter of pandas Series that is created from Spark's Python objects,
    or `pyarrow.Table.to_pandas` method.

    Parameters
    ----------
    data_type : :class:`DataType`
        The data type corresponding to the pandas Series to be converted.
    nullable : bool, optional
        Whether the column is nullable or not. (default ``True``)
    timezone : str, optional
        The timezone to convert from. If there is a timestamp type, it's required.
    struct_in_pandas : str, optional
        How to handle struct type. If there is a struct type, it's required.
        When ``row``, :class:`Row` object will be used.
        When ``dict``, :class:`dict` will be used. If there are duplicated field names,
        The fields will be suffixed, like `a_0`, `a_1`.
        Must be one of: ``row``, ``dict``.
    error_on_duplicated_field_names : bool, optional
        Whether raise an exception when there are duplicated field names.
        (default ``True``)
    timestamp_utc_localized : bool, optional
        Whether the timestamp values are localized to UTC or not.
        The timestamp values from Arrow are localized to UTC,
        whereas the ones from `df.collect()` are localized to the local timezone.
    ndarray_as_list : bool, optional
        Whether `np.ndarray` is converted to a list or not (default ``False``).

    Returns
    -------
    The converter of `pandas.Series`
    """
    import numpy as np
    import pandas as pd
    from pandas.core.dtypes.common import is_datetime64tz_dtype

    pandas_type = _to_corrected_pandas_type(data_type)

    if pandas_type is not None:
        # SPARK-21766: if an integer field is nullable and has null values, it can be
        # inferred by pandas as a float column. If we convert the column with NaN back
        # to integer type e.g., np.int16, we will hit an exception. So we use the
        # pandas-inferred float type, rather than the corrected type from the schema
        # in this case.
        if isinstance(data_type, IntegralType) and nullable:

            def correct_dtype(pser: pd.Series) -> pd.Series:
                if pser.isnull().any():
                    return pser.astype(np.float64, copy=False)
                else:
                    return pser.astype(pandas_type, copy=False)

        elif isinstance(data_type, BooleanType) and nullable:

            def correct_dtype(pser: pd.Series) -> pd.Series:
                if pser.isnull().any():
                    return pser.astype(object, copy=False)
                else:
                    return pser.astype(pandas_type, copy=False)

        elif isinstance(data_type, TimestampType):
            assert timezone is not None

            def correct_dtype(pser: pd.Series) -> pd.Series:
                if not is_datetime64tz_dtype(pser.dtype):
                    pser = pser.astype(pandas_type, copy=False, errors="ignore")
                return _check_series_convert_timestamps_local_tz(pser, timezone=cast(str, timezone))

        else:

            def correct_dtype(pser: pd.Series) -> pd.Series:
                return pser.astype(pandas_type, copy=False)

        return correct_dtype

    def _converter(
        dt: DataType, _struct_in_pandas: Optional[str], _ndarray_as_list: bool
    ) -> Optional[Callable[[Any], Any]]:
        if isinstance(dt, ArrayType):
            _element_conv = _converter(dt.elementType, _struct_in_pandas, _ndarray_as_list)

            if _ndarray_as_list:
                if _element_conv is None:
                    _element_conv = lambda x: x  # noqa: E731

                def convert_array_ndarray_as_list(value: Any) -> Any:
                    if value is None:
                        return None
                    else:
                        # In Arrow Python UDF, ArrayType is converted to `np.ndarray`
                        # whereas a list is expected.
                        return [_element_conv(v) for v in value]  # type: ignore[misc]

                return convert_array_ndarray_as_list
            else:
                if _element_conv is None:
                    return None

                def convert_array_ndarray_as_ndarray(value: Any) -> Any:
                    if value is None:
                        return None
                    elif isinstance(value, np.ndarray):
                        # `pyarrow.Table.to_pandas` uses `np.ndarray`.
                        return np.array([_element_conv(v) for v in value])  # type: ignore[misc]
                    else:
                        assert isinstance(value, list)
                        # otherwise, `list` should be used.
                        return [_element_conv(v) for v in value]  # type: ignore[misc]

                return convert_array_ndarray_as_ndarray

        elif isinstance(dt, MapType):
            _key_conv = _converter(dt.keyType, _struct_in_pandas, _ndarray_as_list) or (lambda x: x)
            _value_conv = _converter(dt.valueType, _struct_in_pandas, _ndarray_as_list) or (lambda x: x)

            def convert_map(value: Any) -> Any:
                if value is None:
                    return None
                elif isinstance(value, list):
                    # `pyarrow.Table.to_pandas` uses `list` of key-value tuple.
                    return {_key_conv(k): _value_conv(v) for k, v in value}
                else:
                    assert isinstance(value, dict)
                    # otherwise, `dict` should be used.
                    return {_key_conv(k): _value_conv(v) for k, v in value.items()}

            return convert_map

        elif isinstance(dt, StructType):
            assert _struct_in_pandas is not None

            field_names = dt.names

            if error_on_duplicated_field_names and len(set(field_names)) != len(field_names):
                raise UnsupportedOperationException(
                    error_class="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
                    message_parameters={"field_names": str(field_names)},
                )

            dedup_field_names = _dedup_names(field_names)

            field_convs = [
                _converter(f.dataType, _struct_in_pandas, _ndarray_as_list) or (lambda x: x) for f in dt.fields
            ]

            if _struct_in_pandas == "row":

                def convert_struct_as_row(value: Any) -> Any:
                    if value is None:
                        return None
                    elif isinstance(value, dict):
                        # `pyarrow.Table.to_pandas` uses `dict`.
                        _values = [field_convs[i](value.get(name, None)) for i, name in enumerate(dedup_field_names)]
                        return _create_row(field_names, _values)
                    else:
                        assert isinstance(value, Row)
                        # otherwise, `Row` should be used.
                        _values = [field_convs[i](value[i]) for i, name in enumerate(value)]
                        return _create_row(field_names, _values)

                return convert_struct_as_row

            elif _struct_in_pandas == "dict":

                def convert_struct_as_dict(value: Any) -> Any:
                    if value is None:
                        return None
                    elif isinstance(value, dict):
                        # `pyarrow.Table.to_pandas` uses `dict`.
                        return {name: field_convs[i](value.get(name, None)) for i, name in enumerate(dedup_field_names)}
                    else:
                        assert isinstance(value, Row)
                        # otherwise, `Row` should be used.
                        return {dedup_field_names[i]: field_convs[i](v) for i, v in enumerate(value)}

                return convert_struct_as_dict

            else:
                raise ValueError(f"Unknown value for `struct_in_pandas`: {_struct_in_pandas}")

        elif isinstance(dt, TimestampType):
            assert timezone is not None

            local_tz: Union[datetime.tzinfo, str] = (
                datetime.timezone.utc if timestamp_utc_localized else _get_local_timezone()
            )

            def convert_timestamp(value: Any) -> Any:
                try:
                    if value is None:
                        return None
                    else:
                        if isinstance(value, datetime.datetime) and value.tzinfo is not None:
                            ts = pd.Timestamp(value)
                        else:
                            ts = pd.Timestamp(value).tz_localize(local_tz)
                except OutOfBoundsDatetime:
                    return pd.to_datetime(value, errors="ignore")
                return ts.tz_convert(timezone).tz_localize(None)

            return convert_timestamp

        elif isinstance(dt, TimestampNTZType):

            def convert_timestamp_ntz(value: Any) -> Any:
                try:
                    if value is None:
                        return None
                    else:
                        return pd.Timestamp(value)
                except OutOfBoundsDatetime:
                    return pd.to_datetime(value, errors="ignore")

            return convert_timestamp_ntz

        elif isinstance(dt, UserDefinedType):
            udt: UserDefinedType = dt

            conv = _converter(udt.sqlType(), _struct_in_pandas="row", _ndarray_as_list=True) or (lambda x: x)

            def convert_udt(value: Any) -> Any:
                if value is None:
                    return None
                elif hasattr(value, "__UDT__"):
                    assert isinstance(value.__UDT__, type(udt))
                    return value
                else:
                    return udt.deserialize(conv(value))

            return convert_udt

        else:
            return None

    conv = _converter(data_type, struct_in_pandas, ndarray_as_list)
    if conv is not None:
        return lambda pser: pser.apply(conv)  # type: ignore[return-value]
    else:
        return lambda pser: pser


def to_pandas(df, jconf) -> pd.DataFrame:
    """
    Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

    This is only available if Pandas is installed and available.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Notes
    -----
    This method should only be used if the resulting Pandas ``pandas.DataFrame`` is
    expected to be small, as all the data is loaded into the driver's memory.

    Usage with ``spark.sql.execution.arrow.pyspark.enabled=True`` is experimental.

    Examples
    --------
    >>> df.toPandas()  # doctest: +SKIP
        age   name
    0    2  Alice
    1    5    Bob
    """
    from pyspark.sql.dataframe import DataFrame

    assert isinstance(df, DataFrame)

    timezone = jconf.sessionLocalTimeZone()

    # Below is toPandas without Arrow optimization.
    rows = df.collect()
    if len(rows) > 0:
        pdf = pd.DataFrame.from_records(rows, index=range(len(rows)), columns=df.columns)  # type: ignore[arg-type]
    else:
        pdf = pd.DataFrame(columns=df.columns)

    # dedup top-level column names
    pdf.columns = _dedup_names(pdf.columns.values.tolist())

    if len(pdf.columns) > 0:
        timezone = jconf.sessionLocalTimeZone()
        # struct_in_pandas = jconf.pandasStructHandlingMode()
        struct_in_pandas = "dict"

        return pd.concat(
            [
                _create_converter_to_pandas(
                    field.dataType,
                    field.nullable,
                    timezone=timezone,
                    struct_in_pandas=("row" if struct_in_pandas == "legacy" else struct_in_pandas),
                    error_on_duplicated_field_names=False,
                    timestamp_utc_localized=False,
                )(pser)
                for (_, pser), field in zip(pdf.items(), df.schema.fields)
            ],
            axis="columns",
        )
    else:
        return pdf


# End of PySpark 3.5.3 copy


def render_grid(pdf, limit):
    # It's important to import DataGrid inside this magic function
    # If you import it at the top of the file it will interfere with
    # the use of DataGrid in a notebook cell. You get a message
    # Loading widget...

    # for every order of magnitude in the limit 10, 100, 1000
    # increase view port height by 10, 20, 30 rows
    # and add 3 rows of padding
    # limit -> num_display_rows
    # 1         -> 3 + 0
    # 10        -> 3 + 10
    # 100       -> 3 + 20
    # 1,000     -> 3 + 30
    # 10,000    -> 3 + 40

    num_display_rows = 3 + math.floor((math.log(limit, 10) * 10))
    base_row_size = 20
    layout_height = f"{num_display_rows * base_row_size}px"

    return DataGrid(
        pdf,
        base_row_size=base_row_size,
        selection_mode="row",
        layout={"height": layout_height},
        header_renderer=TextRenderer(text_wrap=True),
        default_renderer=TextRenderer(text_wrap=True),
    )


def render_ag_grid(pdf):
    grid_options = {
        "columnDefs": [
            # TODO: Remove minWidth workaround and fix the issue where columns have a 0 width if grid is rendered when not visible.
            # The issue is an ipyaggrid issue, see: https://github.com/jupyter-widgets/ipywidgets/issues/2858
            {"headerName": c, "field": c, "sortable": True, "enableRowGroup": True, "autoHeight": True, "minWidth": 100}
            for c in pdf.columns
        ],
        "defaultColDef": DEFAULT_COLUMN_DEF,
        "enableRangeSelection": True,
        "suppressColumnVirtualisation": True,
        "animateRows": True,
    }

    ag_grid_license_key = environ.get("AG_GRID_LICENSE_KEY")

    return Grid(
        grid_data=pdf,
        grid_options=grid_options,
        quick_filter=True,
        theme="ag-theme-balham",
        dark_theme="ag-theme-balham-dark",
        columns_fit="auto",
        index=False,
        license=ag_grid_license_key if ag_grid_license_key else "",
    )


def replchars_to_hex(match):
    return r"\x{0:02x}".format(ord(match.group()))


def rows_to_html(rows, columns, show_nonprinting, truncate):
    html = "<table border='1' class='mathjax_ignore'>\n"
    # generate table head
    html += "<tr><th>%s</th></tr>\n" % "</th><th>".join(map(lambda x: html_escape(x), columns))
    # generate table rows
    for row in rows:
        row = [format_value(str(v), show_nonprinting, truncate) for v in row]
        html += "<tr><td>%s</td></tr>\n" % "</td><td>".join(map(lambda x: html_escape(str(x)), row))
    html += "</table>\n"
    return html


def check_js_integer_safety(data, warnings):
    if data <= JS_MAX_SAFE_INTEGER and data >= JS_MIN_SAFE_INTEGER:
        return data
    else:
        warnings.append(f"int {data} was cast to string to avoid loss of precision.")
        return str(data)


def sanitize_results(data, warnings=[], safe_js_ints=False):
    result = dict()

    if isinstance(data, dict):
        for key, value in data.items():
            result[key] = sanitize_results(value, warnings, safe_js_ints)
    elif isinstance(data, (list, ndarray)):
        json_array = []
        for v in data:
            json_array.append(sanitize_results(v, warnings, safe_js_ints))
        return json_array
    # For Oracle "integers"
    elif isinstance(data, Decimal):
        if data == data.to_integral_value():
            data = int(data)
        if safe_js_ints:
            return check_js_integer_safety(data, warnings)
        else:
            return data
    elif isinstance(data, datetime64):
        return pd.Timestamp(data)
    elif isinstance(data, (bytearray, bytes)):
        return data.hex(" ").upper().split().__str__()
    elif safe_js_ints and isinstance(data, (int)):
        return check_js_integer_safety(data, warnings)
    elif isinstance(data, pt.Row):
        for key, value in data.asDict().items():
            result[key] = sanitize_results(value, warnings, safe_js_ints)
    elif isinstance(data, NamedRowTuple):
        for key, value in zip(data._names, data):
            result[key] = sanitize_results(value, warnings, safe_js_ints)
    else:
        return data
    return result


def format_value(text, show_nonprinting=False, truncate=0):
    formatted_value = text
    if isinstance(formatted_value, str):
        if show_nonprinting:
            formatted_value = replchars.sub(replchars_to_hex, formatted_value)
        if truncate > 0 and len(formatted_value) > truncate:
            formatted_value = formatted_value[:truncate] + "..."
    return formatted_value


# Define a function for conditional conversion of a Pandas df column
# Allows us to have human readable dates & avoid issues like ints
# being cast to something with decimals
def dataframe_conditional_conversion(col):
    if pd.api.types.is_datetime64_any_dtype(col):
        return col.values.astype("datetime64[ns]")
    return col.values.astype(object)


def remove_none_recursive(obj):
    if obj is None:
        return None
    elif isinstance(obj, dict):
        cleaned_dict = {k: v for k, v in ((k, remove_none_recursive(v)) for k, v in obj.items()) if v is not None}
        return cleaned_dict if cleaned_dict else None
    elif isinstance(obj, list):
        cleaned_list = [v for v in (remove_none_recursive(v) for v in obj) if v is not None]
        return cleaned_list if cleaned_list else None
    else:
        return obj
