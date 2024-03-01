import re
import string
from typing import List

import pandas as pd
from IPython.display import HTML, JSON, display

from jupyterlab_sql_editor.outputters.util import (
    dataframe_conditional_conversion,
    format_value,
    make_tag,
    recursive_escape,
    render_ag_grid,
    render_grid,
    render_text,
    rows_to_html,
    sanitize_results,
)

DEFAULT_COLUMN_DEF = {"editable": False, "filter": True, "resizable": True, "sortable": True}

PRINTABLE = string.ascii_letters + string.digits + string.punctuation + " "

replchars = re.compile("([^" + re.escape(PRINTABLE) + "])")

JS_MAX_SAFE_INTEGER = 9007199254740991

JS_MIN_SAFE_INTEGER = -9007199254740991


def _display_results(pdf: pd.DataFrame, output: str, show_nonprinting: bool, truncate: int, args=None) -> None:
    if output == "grid":
        grid(pdf, show_nonprinting, truncate)
    elif output == "aggrid":
        aggrid(pdf, show_nonprinting, truncate)
    elif output == "json":
        expand = args.expand if args else False
        jjson(pdf, show_nonprinting, expand)
    elif output == "html":
        html(pdf, show_nonprinting, truncate)
    elif output == "text":
        text(pdf, truncate)


def aggrid(df: pd.DataFrame, show_nonprinting=False, truncate=256) -> None:
    for c in df.columns:
        df[c] = df[c].apply(lambda v: sanitize_results(v))
        df[c] = df[c].apply(lambda v: format_value(str(v), show_nonprinting, truncate))
    display(render_ag_grid(df))


def grid(df: pd.DataFrame, show_nonprinting=False, truncate=256) -> None:
    for c in df.columns:
        df[c] = df[c].apply(lambda v: sanitize_results(v))
        df[c] = df[c].apply(lambda v: format_value(str(v), show_nonprinting, truncate))
    display(render_grid(df, df.size))


def jjson(df: pd.DataFrame, show_nonprinting=False, expanded=False, date_format="iso") -> None:
    safe_array = []
    warnings: List[str] = []

    valid_date_formats = {"iso", "epoch"}
    if date_format not in valid_date_formats:
        raise ValueError(f"jjson: status must be one of {valid_date_formats}.")

    # sanitize results for display
    for row in df.to_dict(orient="records"):
        safe_array.append(sanitize_results(row, warnings, True))
    if show_nonprinting:
        recursive_escape(safe_array)
    if warnings:
        display(warnings)

    display(
        JSON(
            pd.DataFrame.from_records(safe_array, columns=df.columns).to_json(
                orient="records", date_format=date_format
            ),
            expanded=expanded,
        )
    )


def html(df: pd.DataFrame, show_nonprinting=False, truncate=256) -> None:
    html = rows_to_html(
        sanitize_results(df.apply(dataframe_conditional_conversion).values),
        df.columns.values.tolist(),
        show_nonprinting,
        truncate,
    )
    display(HTML(make_tag("table", False, html)))


def text(df: pd.DataFrame, truncate=256) -> None:
    print(
        render_text(
            sanitize_results(df.apply(dataframe_conditional_conversion).values), df.columns.values.tolist(), truncate
        )
    )
