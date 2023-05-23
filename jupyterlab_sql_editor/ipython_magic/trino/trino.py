import json
import os

import pandas as pd
import trino
from IPython.core.magic import line_cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import HTML, JSON, display
from traitlets import Bool, Instance, Int, Unicode, Union

from jupyterlab_sql_editor.ipython.common import (
    cast_unsafe_ints_to_str,
    escape_control_chars,
    make_tag,
    recursive_escape,
    render_ag_grid,
    render_grid,
    rows_to_html,
)
from jupyterlab_sql_editor.ipython_magic.common.base import Base
from jupyterlab_sql_editor.ipython_magic.trino.trino_export import (
    update_database_schema,
)

VALID_OUTPUTS = ["sql", "text", "json", "html", "aggrid", "grid", "skip", "none"]


@magics_class
class Trino(Base):
    host = Unicode("localhost", config=True, help="Trino server hostname")
    port = Int(443, config=True, help="Trino server port number")
    auth = Instance(
        allow_none=True,
        klass="trino.auth.Authentication",
        config=True,
        help="An instance of the Trino Authentication class",
    )
    user = Unicode(
        "user",
        config=True,
        help="Trino user to use when no authentication is specified. This will set the HTTP header X-Trino-User",
    )
    catalog = Unicode("", config=True, help="Trino catalog to use")
    schema = Unicode("", config=True, help="Trino schema to use")
    httpScheme = Unicode("https", config=True, help="Trino server scheme https/http")
    verify = Union(
        [Bool(), Unicode()],
        default_value=True,
        config=True,
        help="Trino SSL verification. True or False to enable or disable SSL verification. /path/to/cert.crt for self-signed certificate.",
    )
    conn = None
    cur = None

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument("sql", nargs="*", type=str, help="SQL statement to execute")
    @argument(
        "-l",
        "--limit",
        metavar="max_rows",
        type=int,
        help="The maximum number of rows to display. A value of zero is equivalent to `--output skip`",
    )
    @argument(
        "-r",
        "--refresh",
        metavar="all|none",
        type=str,
        default="none",
        help="Force the regeneration of the schema cache file.",
    )
    @argument("-d", "--dataframe", metavar="name", type=str, help="Capture results in pandas dataframe")
    @argument(
        "-o",
        "--output",
        metavar="sql|json|html|aggrid|grid|text|skip|none",
        type=str,
        default="html",
        help="Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)",
    )
    @argument(
        "-s",
        "--show-nonprinting",
        action="store_true",
        help="Replace none printable characters with their ascii codes (LF -> \x0a)",
    )
    @argument(
        "-x",
        "--raw",
        action="store_true",
        help="Run statement as is. Do not wrap statement with a limit. \
                    Use this option to run statement which can't be wrapped in a SELECT/LIMIT statement. \
                    For example EXPLAIN, SHOW TABLE, SHOW CATALOGS.",
    )
    @argument("-c", "--catalog", metavar="catalogname", default=None, type=str, help="Trino catalog to use")
    @argument("-m", "--schema", metavar="schemaname", default=None, type=str, help="Trino schema to use")
    @argument("-j", "--jinja", action="store_true", help="Enable Jinja templating support")
    @argument("-t", "--truncate", metavar="max_cell_length", type=int, help="Truncate output")
    @argument("--expand", action="store_true", help="Expand json results")
    def trino(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %trino and as %%trino"
        self.set_user_ns(local_ns)
        args = parse_argstring(self.trino, line)
        output_file = self.outputFile or f"{os.path.expanduser('~')}/.local/trinodb.schema.json"

        truncate = 256
        if args.truncate and args.truncate > 0:
            truncate = args.truncate

        catalog = args.catalog
        if not catalog:
            catalog = self.catalog

        schema = args.schema
        if not schema:
            schema = self.schema

        limit = args.limit
        if limit is None:
            limit = self.limit

        catalog_array = self.get_catalog_array()
        if self.check_refresh(args.refresh.lower(), output_file, catalog_array):
            return

        output = args.output.lower()
        if output not in VALID_OUTPUTS:
            print(f"Invalid output option {args.output}. The valid options are {VALID_OUTPUTS}.")
            return

        if limit <= 0 or output == "skip" or output == "none":
            print("Query execution skipped")
            return

        sql = self.get_sql_statement(cell, args.sql, args.jinja)
        if not sql:
            return

        if output == "sql":
            return self.display_sql(sql)
        elif not args.raw:
            sql = f"{sql} limit {limit+1}"

        self.conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            auth=self.auth,
            user=self.user,
            catalog=catalog,
            schema=schema,
            http_scheme=self.httpScheme,
            verify=self.verify,
        )
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        results = self.cur.fetchmany(limit + 1)
        columns = list(map(lambda d: d[0], self.cur.description))

        if len(results) > limit:
            print("Only showing top %d row(s)" % limit)
            results = results[:limit]

        results = list(map(lambda row: [self.format_cell(v, output, truncate) for v in row], results[:limit]))

        if args.dataframe:
            print(f"Saved results to pandas dataframe named `{args.dataframe}`")
            pdf = pd.DataFrame.from_records(results, columns=columns)
            self.shell.user_ns.update({args.dataframe: pdf})

        self.display_results(
            results=results,
            columns=columns,
            output=output,
            limit=limit,
            show_nonprinting=args.show_nonprinting,
            args=args,
        )

    def display_results(self, results, columns, output, limit=20, show_nonprinting=False, args=None):
        if output == "grid":
            pdf = pd.DataFrame.from_records(results, columns=columns)
            if show_nonprinting:
                for c in pdf.columns:
                    pdf[c] = pdf[c].apply(lambda v: escape_control_chars(str(v)))
            display(render_grid(pdf, limit))
        elif output == "aggrid":
            pdf = pd.DataFrame.from_records(results, columns=columns)
            if show_nonprinting:
                for c in pdf.columns:
                    pdf[c] = pdf[c].apply(lambda v: escape_control_chars(str(v)))
            display(render_ag_grid(pdf))
        elif output == "json":
            json_array = []
            warnings = []
            json_string = pd.DataFrame.from_records(results, columns=columns).to_json(orient="records")
            json_dict = json.loads(json_string)
            # cast unsafe ints to str for display
            for row in json_dict:
                json_array.append(cast_unsafe_ints_to_str(row, warnings))
            if show_nonprinting:
                recursive_escape(json_array)
            display(warnings, JSON(json_array, expanded=args.expand))
        elif output == "html":
            html = rows_to_html(columns, results, show_nonprinting)
            display(HTML(make_tag("table", False, html)))
        elif output == "text":
            print(self.render_text(results, columns))

    def check_refresh(self, refresh_arg, output_file, catalog_array):
        if refresh_arg == "all":
            update_database_schema(self.cur, output_file, catalog_array)
            return True
        if refresh_arg != "none":
            print(f"Invalid refresh option given {refresh_arg}. Valid refresh options are [all|local|none]")
        return False

    @staticmethod
    def format_cell(v, output="html", truncate=256):
        if output != "json" and isinstance(v, str):
            if len(v) > truncate:
                v = v[:truncate] + "..."
        return v

    @staticmethod
    def render_text(rows, columns):
        string_builder = ""
        num_cols = len(columns)
        # We set a minimum column width at '3'
        minimum_col_width = 3

        # Initialise the width of each column to a minimum value
        col_widths = [minimum_col_width for i in range(num_cols)]

        # Compute the width of each column
        for i, column in enumerate(columns):
            col_widths[i] = max(col_widths[i], len(column))
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        padded_columns = []
        for i, column in enumerate(columns):
            new_name = column.ljust(col_widths[i], " ")
            padded_columns.append(new_name)

        padded_rows = []
        for row in rows:
            new_row = []
            for i, cell in enumerate(row):
                cell_value = str(cell)
                new_val = cell_value.ljust(col_widths[i], " ")
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

    @staticmethod
    def display_link():
        link = "http://localhost"
        app_name = "name"
        display(HTML(f"""<a class="external" href="{link}" target="_blank" >Open Spark UI ⭐ {app_name}</a>"""))
