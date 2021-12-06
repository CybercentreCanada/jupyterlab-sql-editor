import time
import math
import os
import re
import string
from html import escape

import IPython
from IPython.core.magic import Magics, magics_class
from jinja2 import Template, StrictUndefined
from traitlets import Int, Unicode, Bool

DEFAULT_SCHEMA_TTL = -1
DEFAULT_CATALOGS = ''


VARIABLE_NOT_FOUND_MSG = '''
A Jinja template variable named {{{var_name}}} was located in your SQL statement.

However Jinja was unable to substitute it's value because the variable "{var_name}" was not found in your ipython kernel.

Option 1: If you intended to use a template variable make sure to assign a value to "{var_name}"
'''

HOW_TO_ESCAPE_MSG = '''
Option 2: If you intended to include "{{" in your statement then you'll need to escape this special Jinja variable delimiter.

To have Jinja ignore parts it would otherwise handle as variables or blocks. For example, if, with the default syntax, you want to use {{ as a raw string in a template and not start a variable, you have to use a trick.

The easiest way to output a literal variable delimiter "{{" is by using a variable expression:

{{ '{{' }}

For bigger sections, it makes sense to mark a block raw. For example, to include example Jinja syntax in a template, you can use this snippet:

%%trino --limit 3
{% raw %}
/*
This is a comment which happens to contain a jinja template
variable {{x}} that we want to keep as is.
*/
{% endraw %}

SELECT
    *
FROM
    {{ table_name }}

'''

RAISING_ERROR_MSG = "Raising an error to prevent statement from being executed incorrectly."

class ExplainUndefined(StrictUndefined):
    __slots__ = ()

    def __str__(self) -> str:
        print(VARIABLE_NOT_FOUND_MSG.format(var_name=self._undefined_name))
        print(HOW_TO_ESCAPE_MSG)
        print(RAISING_ERROR_MSG)
        return super().__str__(self)

PRINTABLE = string.ascii_letters + string.digits + string.punctuation + ' '

replchars = re.compile('([^' + re.escape(PRINTABLE) + '])')


@magics_class
class Base(Magics):
    limit = Int(20, config=True, help='The maximum number of rows to display')
    cacheTTL = Int(DEFAULT_SCHEMA_TTL, config=True, help=f'Re-generate output schema file if older than time specified (defaults to {DEFAULT_SCHEMA_TTL} seconds)')
    catalogs = Unicode(DEFAULT_CATALOGS, config=True, help=f'Retrive schema from the specified list of catalogs (defaults to "{DEFAULT_CATALOGS}")')
    interactive = Bool(False, config=True, help='Display results in interactive grid')
    outputFile = Unicode('', config=True, help='Output schema to specified file')

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.user_ns = {}

    def make_tag(self, tag_name, show_nonprinting, body='', **kwargs):
        if show_nonprinting:
            body = self.escape_control_chars(escape(body))
        attributes = ' '.join(map(lambda x: '%s="%s"' % x, kwargs.items()))
        if attributes:
            return f'<{tag_name} {attributes}>{body}</{tag_name}>'
        else:
            return f'<{tag_name}>{body}</{tag_name}>'

    @staticmethod
    def bind_variables(query, user_ns):
        template = Template(query, undefined=ExplainUndefined)
        return template.render(user_ns)

    def get_catalog_array(self):
        catalog_array = []
        if ',' in self.catalogs:
            catalog_array = self.catalogs.split(',')
        return catalog_array

    def get_sql_statement(self, cell, sql_argument):
        sql = cell
        if cell is None:
            sql = ' '.join(sql_argument)
        if not sql:
            print('No sql statement to execute')
        else:
            sql = self.bind_variables(sql, self.user_ns)
        return sql

    def set_user_ns(self, local_ns):
        if local_ns is None:
            local_ns = {}

        self.user_ns = self.shell.user_ns.copy()
        self.user_ns.update(local_ns)

    @staticmethod
    def should_update_schema(schema_file_name, refresh_threshold):
        file_exists = os.path.isfile(schema_file_name)
        ttl_expired = False
        if file_exists:
            file_time = os.path.getmtime(schema_file_name)
            current_time = time.time()
            if current_time - file_time > refresh_threshold:
                print(f'TTL {refresh_threshold} seconds expired, re-generating schema file: {schema_file_name}')
                ttl_expired = True

        return (not file_exists) or ttl_expired

    @staticmethod
    def render_grid(pdf, limit):
        # It's important to import DataGrid inside this magic function
        # If you import it at the top of the file it will interfere with
        # the use of DataGrid in a notebook cell. You get a message
        # Loading widget...
        from ipydatagrid import DataGrid
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
        return DataGrid(pdf, base_row_size=base_row_size, selection_mode="row", layout={"height": layout_height})

    def display_sql(self, sql):
        def _jupyterlab_repr_html_(self):
            from pygments import highlight
            from pygments.formatters.html import HtmlFormatter

            fmt = HtmlFormatter()
            style = "<style>{}\n{}</style>".format(
                fmt.get_style_defs(".output_html"), fmt.get_style_defs(".jp-RenderedHTML")
            )
            return style + highlight(self.data, self._get_lexer(), fmt)

        # Replace _repr_html_ with our own version that adds the 'jp-RenderedHTML' class
        # in addition to 'output_html'.
        IPython.display.Code._repr_html_ = _jupyterlab_repr_html_
        return IPython.display.Code(data=sql, language="mysql")

    @staticmethod
    def replchars_to_hex(match):
        return r'\x{0:02x}'.format(ord(match.group()))

    def escape_control_chars(self, text):
        return replchars.sub(self.replchars_to_hex, text)

    def recursive_escape(self, input):
        # check whether it's a dict, list, tuple, or scalar
        if isinstance(input, dict):
            items = input.items()
        elif isinstance(input, (list, tuple)):
            items = enumerate(input)
        else:
            # just a value, split and return
            return self.escape_control_chars(str(input))

        # now call ourself for every value and replace in the input
        for key, value in items:
            input[key] = self.recursive_escape(value)
        return input
