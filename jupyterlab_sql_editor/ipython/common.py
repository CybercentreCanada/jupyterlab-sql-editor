import math
import re
import string
from html import escape
from html import escape as html_escape
from os import environ, listdir
from os.path import isdir, join

from ipydatagrid import DataGrid, TextRenderer

PRINTABLE = string.ascii_letters + string.digits + string.punctuation + " "

replchars = re.compile("([^" + re.escape(PRINTABLE) + "])")


def make_tag(tag_name, show_nonprinting, body="", **kwargs):
    body = str(body)
    if show_nonprinting:
        body = escape_control_chars(escape(body))
    attributes = " ".join(map(lambda x: '%s="%s"' % x, kwargs.items()))
    if attributes:
        return f"<{tag_name} {attributes}>{body}</{tag_name}>"
    else:
        return f"<{tag_name}>{body}</{tag_name}>"


def escape_control_chars(text):
    return replchars.sub(replchars_to_hex, text)


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


def replchars_to_hex(match):
    return r"\x{0:02x}".format(ord(match.group()))


def recursive_escape(input):
    # check whether it's a dict, list, tuple, or scalar
    if isinstance(input, dict):
        items = input.items()
    elif isinstance(input, (list, tuple)):
        items = enumerate(input)
    else:
        # just a value, split and return
        return escape_control_chars(str(input))

    # now call ourself for every value and replace in the input
    for key, value in items:
        input[key] = recursive_escape(value)
    return input


def rows_to_html(columns, row_data, show_nonprinting):
    html = "<table border='1'>\n"
    # generate table head
    html += "<tr><th>%s</th></tr>\n" % "</th><th>".join(map(lambda x: html_escape(x), columns))
    # generate table rows
    for row in row_data:
        if show_nonprinting:
            row = [escape_control_chars(str(v)) for v in row]
        html += "<tr><td>%s</td></tr>\n" % "</td><td>".join(map(lambda x: html_escape(x), row))
    html += "</table>\n"
    return html


def find_nvm_lib_dirs():
    NVM_VERSIONS_SUBPATH = "/versions/node/"
    nvm_dir = environ["NVM_DIR"]
    dirs = []
    if nvm_dir:
        dirs = [
            f"{nvm_dir}{NVM_VERSIONS_SUBPATH}{d}/lib"
            for d in listdir(nvm_dir + NVM_VERSIONS_SUBPATH)
            if isdir(join(nvm_dir + NVM_VERSIONS_SUBPATH, d))
        ]
    return dirs
