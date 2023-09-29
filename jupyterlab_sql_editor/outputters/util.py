import math
import re
import string
from collections import Counter
from html import escape
from html import escape as html_escape
from os import environ
from typing import List, Optional, Type
from warnings import catch_warnings, simplefilter

import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.types as pt
from ipyaggrid import Grid
from ipydatagrid import DataGrid, TextRenderer
from pandas.core.dtypes.common import is_timedelta64_dtype
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DataType,
    DayTimeIntervalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntegralType,
    LongType,
    ShortType,
    TimestampNTZType,
    TimestampType,
    cast,
)
from trino.client import NamedRowTuple

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


def _to_corrected_pandas_type(dt: DataType) -> Optional[Type]:
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
        return np.datetime64
    elif type(dt) == TimestampNTZType:
        return np.datetime64
    elif type(dt) == DayTimeIntervalType:
        return np.timedelta64
    else:
        return None


def _check_series_convert_timestamps_localize(s, from_timezone: Optional[str], to_timezone: Optional[str]):
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
        try:
            return cast(
                "PandasSeriesLike",
                s.apply(
                    lambda ts: ts.tz_localize(from_tz, ambiguous=False).tz_convert(to_tz).tz_localize(None)
                    if ts is not pd.NaT
                    else pd.NaT
                ),
            )
        except Exception as exc:
            print(f"{from_tz} {to_tz} {exc}")
    else:
        return s


def _check_series_convert_timestamps_local_tz(s, timezone: str):
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


# spark
def to_pandas(df: pyspark.sql.DataFrame, sparkSession) -> pd.DataFrame:
    jconf = sparkSession._jconf
    timezone = jconf.sessionLocalTimeZone()

    # Below is toPandas without Arrow optimization.
    pdf = pd.DataFrame.from_records(df.collect(), columns=df.columns)
    column_counter = Counter(df.columns)

    corrected_dtypes: List[Optional[Type]] = [None] * len(df.schema)
    for index, field in enumerate(df.schema):
        # We use `iloc` to access columns with duplicate column names.
        if column_counter[field.name] > 1:
            pandas_col = pdf.iloc[:, index]
        else:
            pandas_col = pdf[field.name]

        pandas_type = _to_corrected_pandas_type(field.dataType)
        # SPARK-21766: if an integer field is nullable and has null values, it can be
        # inferred by pandas as a float column. If we convert the column with NaN back
        # to integer type e.g., np.int16, we will hit an exception. So we use the
        # pandas-inferred float type, rather than the corrected type from the schema
        # in this case.
        if pandas_type is not None and not (
            isinstance(field.dataType, IntegralType) and field.nullable and pandas_col.isnull().any()
        ):
            corrected_dtypes[index] = pandas_type
        # Ensure we fall back to nullable numpy types.
        if isinstance(field.dataType, IntegralType) and pandas_col.isnull().any():
            corrected_dtypes[index] = np.float64
        if isinstance(field.dataType, BooleanType) and pandas_col.isnull().any():
            corrected_dtypes[index] = object

    new_df = pd.DataFrame()
    for index, t in enumerate(corrected_dtypes):
        column_name = df.schema[index].name

        # We use `iloc` to access columns with duplicate column names.
        if column_counter[column_name] > 1:
            series = pdf.iloc[:, index]
        else:
            series = pdf[column_name]

        # No need to cast for non-empty series for timedelta. The type is already correct.
        should_check_timedelta = is_timedelta64_dtype(t) and len(pdf) == 0

        if (t is not None and not is_timedelta64_dtype(t)) or should_check_timedelta:
            series = series.astype(t, copy=False, errors="ignore")

        with catch_warnings():
            from pandas.errors import PerformanceWarning

            simplefilter(action="ignore", category=PerformanceWarning)
            # `insert` API makes copy of data,
            # we only do it for Series of duplicate column names.
            # `pdf.iloc[:, index] = pdf.iloc[:, index]...` doesn't always work
            # because `iloc` could return a view or a copy depending by context.
            if column_counter[column_name] > 1:
                new_df.insert(index, column_name, series, allow_duplicates=True)
            else:
                new_df[column_name] = series

    if timezone is None:
        return new_df
    else:
        for field in df.schema:
            # TODO: handle nested timestamps, such as ArrayType(TimestampType())?
            if isinstance(field.dataType, TimestampType):
                new_df[field.name] = _check_series_convert_timestamps_local_tz(new_df[field.name], timezone)
        return new_df


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
            {"headerName": c, "field": c, "sortable": True, "enableRowGroup": True, "autoHeight": True}
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
        columns_fit="auto",
        index=False,
        license=ag_grid_license_key if ag_grid_license_key else "",
    )


def replchars_to_hex(match):
    return r"\x{0:02x}".format(ord(match.group()))


def rows_to_html(rows, columns, show_nonprinting, truncate):
    html = "<table border='1'>\n"
    # generate table head
    html += "<tr><th>%s</th></tr>\n" % "</th><th>".join(map(lambda x: html_escape(x), columns))
    # generate table rows
    for row in rows:
        row = [format_value(str(v), show_nonprinting, truncate) for v in row]
        html += "<tr><td>%s</td></tr>\n" % "</td><td>".join(map(lambda x: html_escape(str(x)), row))
    html += "</table>\n"
    return html


def sanitize_results(data, warnings=[], safe_js_ints=False):
    result = dict()

    if isinstance(data, dict):
        for key, value in data.items():
            result[key] = sanitize_results(value, warnings)
    elif isinstance(data, list):
        json_array = []
        for v in data:
            json_array.append(sanitize_results(v, warnings))
        return json_array
    elif isinstance(data, (bytearray, bytes)):
        return data.hex(" ").upper().split().__str__()
    elif safe_js_ints and isinstance(data, int):
        if data <= JS_MAX_SAFE_INTEGER and data >= JS_MIN_SAFE_INTEGER:
            return data
        else:
            warnings.append(f"int {data} was cast to string to avoid loss of precision.")
            return str(data)
    elif isinstance(data, pt.Row):
        for key, value in data.asDict().items():
            result[key] = sanitize_results(value, warnings)
    elif isinstance(data, NamedRowTuple):
        for key, value in zip(data._names, data):
            result[key] = sanitize_results(value, warnings)
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
