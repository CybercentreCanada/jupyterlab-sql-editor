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
from traitlets import Instance, Int, Unicode

VALID_OUTPUTS = ['sql', 'text', 'json', 'html', 'grid', 'skip', 'none']

@magics_class
class Trino(Base):
    host = Unicode('localhost', config=True, help='The trino server hostname')
    port = Int(443, config=True, help='Trino server port number')
    httpScheme = Unicode('https', config=True, help='Trino server scheme https/http')
    auth = Instance(allow_none=True, klass='trino.auth.Authentication', config=True, help='An instance of the Trino Authentication class')
    user = Unicode('user', config=True, help='Trino user to use when no authentication is specified. This will set the HTTP header X-Trino-User')
    catalog = Unicode("", config=True, help='Trino catalog to use')
    schema = Unicode("", config=True, help='Trino schema to use')
    conn = None
    cur = None

    def __init__(self, shell=None, line=None, local_ns=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.args = parse_argstring(self.trino, line)
        self.output = self.args.output.lower()
        if self.outputFile is None:
            self.outputFile = f"{os.path.expanduser('~')}/.local/trinodb.schema.json"
        if self.args.truncate and self.args.truncate > 0:
            self.truncate = self.args.truncate
        else:
            self.truncate = 256
        self.set_user_ns(local_ns)

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
    def trino(self, cell=None):
        "Magic that works both as %trino and as %%trino"
        catalog = self.args.catalog
        if not catalog:
            catalog = self.catalog
        print(catalog)
        schema = self.args.schema
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
            http_scheme=self.httpScheme)
        self.cur = self.conn.cursor()

        catalog_array = self.get_catalog_array()
        if self.check_refresh(self.args.refresh, self.outputFile, catalog_array):
            return

        sql = self.get_sql_statement(cell, self.args.sql, self.args.jinja)
        if not sql:
            return

        limit = self.args.limit
        if limit is None:
            limit = self.limit

        if not self.output in VALID_OUTPUTS:
            print(f'Invalid output option {self.output}. The valid options are {VALID_OUTPUTS}.')
            return

        if self.output == 'sql':
            return self.display_sql(sql)
        if self.output == 'json':
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
        if not self.args.raw is True:
            sql = f'{sql} limit {limit+1}'

        self.cur.execute(sql)
        results = self.cur.fetchmany(limit+1)

        columns = list(map(lambda d: d[0], self.cur.description))

        more_results = False
        if len(results) > limit:
            more_results = True
            results = results[:limit]

        if self.args.dataframe:
            print(f'Saved results to pandas dataframe named `{self.args.dataframe}`')
            pdf = pd.DataFrame.from_records(results, columns=columns)
            self.shell.user_ns.update({self.args.dataframe: pdf})

        if limit <= 0 or self.output == 'skip' or self.output == 'none':
            print('Query execution skipped')
            return

        def format_cell(cell_value):
            cell_value = str(cell_value) if cell_value else "null"
            if self.output != 'json' and len(cell_value) > self.truncate:
                cell_value = cell_value[:self.truncate] + "..."
            return cell_value
        results = list(map(lambda row: [format_cell(value) for value in row], results[:limit]))

        if self.output == 'grid':
            pdf = pd.DataFrame.from_records(results, columns=columns)
            if self.args.show_nonprinting:
                for column in pdf.columns:
                    pdf[column] = pdf[column].apply(lambda v: escape_control_chars(str(v)))
            display(render_grid(pdf, limit))
        elif self.output == 'json':
            json_array = []
            for row in results:
                python_obj = {}
                for idx, column_name in enumerate(columns):
                    python_val = None
                    if row[idx]:
                        python_val = json.loads(row[idx])
                    python_obj[column_name] = python_val
                json_array.append(python_obj)
            if self.args.show_nonprinting:
                recursive_escape(json_array)
            display(JSON(json_array))
        elif self.output == 'html':
            html = rows_to_html(columns, results, self.args.show_nonprinting)
            display(HTML(make_tag('table', False, html)))
        elif self.output == 'text':
            print(self.render_text(results, columns))

        if more_results:
            print('Only showing top %d row(s)' % limit)

    def check_refresh(self, refresh_arg, output_file, catalog_array):
        if refresh_arg.lower() == 'all':
            update_database_schema(self.cur, output_file, catalog_array)
            return True
        if refresh_arg.lower() != 'none':
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
