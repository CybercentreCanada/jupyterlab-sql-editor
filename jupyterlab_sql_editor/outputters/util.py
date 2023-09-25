import math
import re
import string
from html import escape
from html import escape as html_escape
from os import environ

from ipyaggrid import Grid
from ipydatagrid import DataGrid, TextRenderer
from pyspark.sql.types import Row
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
    elif isinstance(data, Row):
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
