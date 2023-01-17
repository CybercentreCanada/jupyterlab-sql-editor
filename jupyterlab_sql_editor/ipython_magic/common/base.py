import os
import time

import IPython
from IPython.core.magic import Magics, magics_class
from jinja2 import StrictUndefined, Template
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from traitlets import Bool, Int, Unicode

DEFAULT_SCHEMA_TTL = -1
DEFAULT_CATALOGS = ""


VARIABLE_NOT_FOUND_MSG = """
A Jinja template variable named {{{var_name}}} was located in your SQL statement.

However Jinja was unable to substitute it's value because the variable "{var_name}" was not found in your ipython kernel.

Option 1: If you intended to use a template variable make sure to assign a value to "{var_name}"
"""

HOW_TO_ESCAPE_MSG = """
Option 2: If you intended to include "{{" in your statement then you'll need to escape this special Jinja variable delimiter.

To have Jinja ignore parts it would otherwise handle as variables or blocks. For example, if, with the default syntax,
you want to use {{ as a raw string in a template and not start a variable, you have to use a trick.

The easiest way to output a literal variable delimiter "{{" is by using a variable expression:

{{ '{{' }}

For bigger sections, it makes sense to mark a block raw. For example, to include example Jinja syntax in a template,
you can use this snippet:

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

"""

RAISING_ERROR_MSG = "Raising an error to prevent statement from being executed incorrectly."


class ExplainUndefined(StrictUndefined):
    __slots__ = ()

    def __str__(self):
        print(VARIABLE_NOT_FOUND_MSG.format(var_name=self._undefined_name))
        print(HOW_TO_ESCAPE_MSG)
        print(RAISING_ERROR_MSG)
        super().__str__(self)


@magics_class
class Base(Magics):
    limit = Int(20, config=True, help="The maximum number of rows to display")
    cacheTTL = Int(
        DEFAULT_SCHEMA_TTL,
        config=True,
        help=f"Re-generate output schema file if older than time specified (defaults to {DEFAULT_SCHEMA_TTL} minutes)",
    )
    catalogs = Unicode(
        DEFAULT_CATALOGS,
        config=True,
        help=f'Retrive schema from the specified list of catalogs (defaults to "{DEFAULT_CATALOGS}")',
    )
    interactive = Bool(False, config=True, help="Display results in interactive grid")
    outputFile = Unicode("", config=True, help="Output schema to specified file")

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.user_ns = {}

    @staticmethod
    def bind_variables(query, user_ns):
        template = Template(query, undefined=ExplainUndefined)
        return template.render(user_ns)

    def get_catalog_array(self):
        catalog_array = []
        if "," in self.catalogs:
            catalog_array = self.catalogs.split(",")
        return catalog_array

    def get_sql_statement(self, cell, sql_argument, use_jinja):
        sql = cell
        if cell is None:
            sql = " ".join(sql_argument)
        if not sql:
            print("No sql statement to execute")
        elif use_jinja:
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
            if current_time - file_time > (refresh_threshold * 60):
                print(f"TTL {refresh_threshold} minutes expired, re-generating schema file: {schema_file_name}")
                ttl_expired = True

        return (not file_exists) or ttl_expired

    def display_sql(self, sql):
        def _jupyterlab_repr_html_(self):
            fmt = HtmlFormatter()
            style = "<style>{}\n{}</style>".format(
                fmt.get_style_defs(".output_html"), fmt.get_style_defs(".jp-RenderedHTML")
            )
            return style + highlight(self.data, self._get_lexer(), fmt)

        # Replace _repr_html_ with our own version that adds the 'jp-RenderedHTML' class
        # in addition to 'output_html'.
        IPython.display.Code._repr_html_ = _jupyterlab_repr_html_
        return IPython.display.Code(data=sql, language="mysql")
