import json
import os

import pandas as pd
import trino

from IPython.display import display, HTML, JSON
from IPython.core.magic import line_cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from jupyterlab_sql_editor.ipython.common import escape_control_chars, make_tag, recursive_escape, render_grid, rows_to_html
from jupyterlab_sql_editor.ipython_magic.common.base import Base
from jupyterlab_sql_editor.ipython_magic.trino.trino_export import update_database_schema
from traitlets import Bool, Instance, Int, Unicode, Union

VALID_OUTPUTS = ['sql', 'text', 'json', 'html', 'grid', 'skip', 'none']

@magics_class
class Trino(Base):
    host = Unicode('localhost', config=True, help='Trino server hostname')
    port = Int(443, config=True, help='Trino server port number')
    auth = Instance(allow_none=True, klass='trino.auth.Authentication', config=True, help='An instance of the Trino Authentication class')
    user = Unicode('user', config=True, help='Trino user to use when no authentication is specified. This will set the HTTP header X-Trino-User')
    catalog = Unicode("", config=True, help='Trino catalog to use')
    schema = Unicode("", config=True, help='Trino schema to use')
    httpScheme = Unicode('https', config=True, help='Trino server scheme https/http')
    verify = Union([Bool(), Unicode()], default_value=True, config=True,
                  help='Trino SSL verification. True or False to enable or disable SSL verification. /path/to/cert.crt for self-signed certificate.')
    conn = None
    cur = None

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement to execute')
    @argument('-l', '--limit', metavar='max_rows', type=int, help='The maximum number of rows to display. A value of zero is equivalent to `--output skip`')
    @argument('-r', '--refresh', metavar='all|local|none', type=str, default='none',
              help='Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.')
    @argument('-d', '--dataframe', metavar='name', type=str, help='Capture results in pandas dataframe')
    @argument('-o', '--output', metavar='sql|json|html|grid|text|skip|none', type=str, default='html',
              help='Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)')
    @argument('-s', '--show-nonprinting', action='store_true', help='Replace none printable characters with their ascii codes (LF -> \x0a)')
    @argument('-x', '--raw', action='store_true',
              help="Run statement as is. Do not wrap statement with a limit. \
                    Use this option to run statement which can't be wrapped in a SELECT/LIMIT statement. \
                    For example EXPLAIN, SHOW TABLE, SHOW CATALOGS.")
    @argument('-c', '--catalog', metavar='catalogname', default=None, type=str, help='Trino catalog to use')
    @argument('-m', '--schema', metavar='schemaname', type=str, help='Trino schema to use')
    @argument('-j', '--jinja', action='store_true', help='Enable Jinja templating support')
    @argument('-t', '--truncate', metavar='max_cell_length', type=int, help='Truncate output')
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
        print(catalog)
        schema = args.schema
        if not schema:
            schema = self.schema
        print(schema)

        self.conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            auth=self.auth,
            user=self.user,
            catalog=catalog,
            schema=schema,
            http_scheme=self.httpScheme,
            verify=self.verify
        )
        self.cur = self.conn.cursor()

        catalog_array = self.get_catalog_array()
        if self.check_refresh(args.refresh.lower(), output_file, catalog_array):
            return

        sql = self.get_sql_statement(cell, args.sql, args.jinja)
        if not sql:
            return

        limit = args.limit
        if limit is None:
            limit = self.limit

        output = args.output.lower()

        if not output in VALID_OUTPUTS:
            print(f'Invalid output option {args.output}. The valid options are {self.valid_outputs}.')
            return

        if output == 'sql':
            return self.display_sql(sql)
        elif output == 'json':
            # Determine the resulting column names
            self.cur.execute(f'SHOW STATS FOR ({sql})')
            # Assume a maximum possible number of columns of 100000
            results = self.cur.fetchmany(100000)
            column_names = []
            for idx, row in enumerate(results):
                if row[0]:
                    column_names.append(row[0])
            # Cast every column to JSON
            select_exprs = []
            for column_name in column_names:
                select_exprs.append(f'CAST({column_name} AS JSON) AS "{column_name}"')
            select = ','.join(select_exprs)
            sql = f'select {select} from ({sql}) limit {limit+1}'
        elif not args.raw is True:
            sql = f'{sql} limit {limit+1}'

        self.cur.execute(sql)
        results = self.cur.fetchmany(limit+1)

        columns = list(map(lambda d: d[0], self.cur.description))

        more_results = False
        if len(results) > limit:
            more_results = True
            results = results[:limit]

        if args.dataframe:
            print(f'Saved results to pandas dataframe named `{args.dataframe}`')
            pdf = pd.DataFrame.from_records(results, columns=columns)
            self.shell.user_ns.update({args.dataframe: pdf})

        if limit <= 0 or output == 'skip' or output == 'none':
            print('Query execution skipped')
            return

        def format_cell(v):
            v = str(v) if v else "null"
            if output != 'json' and len(v) > truncate:
                v = v[:truncate] + "..."
            return v
        results = list(map(lambda row: [format_cell(v) for v in row], results[:limit]))

        if output == 'grid':
            pdf = pd.DataFrame.from_records(results, columns=columns)
            if args.show_nonprinting:
                for c in pdf.columns:
                    pdf[c] = pdf[c].apply(lambda v: escape_control_chars(str(v)))
            display(render_grid(pdf, limit))
        elif output == 'json':
            json_array = []
            for row in results:
                python_obj = {}
                for idx, column_name in enumerate(columns):
                    python_val = None
                    if row[idx]:
                        python_val = json.loads(row[idx])
                    python_obj[column_name] = python_val
                json_array.append(python_obj)
            if args.show_nonprinting:
                recursive_escape(json_array)
            display(JSON(json_array))
        elif output == 'html':
            html = rows_to_html(columns, results, args.show_nonprinting)
            display(HTML(make_tag('table', False, html)))
        elif output == 'text':
            print(self.render_text(results, columns))

        if more_results:
            print('Only showing top %d row(s)' % limit)

    def check_refresh(self, refresh_arg, output_file, catalog_array):
        if refresh_arg == 'all':
            update_database_schema(self.cur, output_file, catalog_array)
            return True
        if refresh_arg != 'none':
            print(f'Invalid refresh option given {refresh_arg}. Valid refresh options are [all|local|none]')
        return False

    @staticmethod
    def render_text(rows, columns):
        string_builder = ''
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
            new_name = column.ljust(col_widths[i], ' ')
            padded_columns.append(new_name)

        padded_rows = []
        for row in rows:
            new_row = []
            for i, cell in enumerate(row):
                cell_value = str(cell)
                new_val = cell_value.ljust(col_widths[i], ' ')
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
