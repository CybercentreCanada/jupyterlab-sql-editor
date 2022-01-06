from html import escape
import json
import os

import pandas as pd
import trino
from IPython.core.display import HTML, JSON
from IPython.core.magic import Magics, line_cell_magic, line_magic, cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from traitlets import Int, Unicode, Instance

from cccs.ipython.common import make_tag, recursive_escape, render_grid
from ipython_magic.common.base import Base
from ipython_magic.trino.trino_export import update_database_schema

@magics_class
class Trino(Base):
    host = Unicode('localhost', config=True, help='The trino server hostname')
    port = Int(443, config=True, help='Trino server port number')
    httpScheme = Unicode('https', config=True, help='Trino server scheme https/http')
    auth = Instance(allow_none=True, klass='trino.auth.Authentication', config=True, help='An instance of the Trino Authentication class')
    user = Unicode('user', config=True, help='Trino user to use when no authentication is specified. This will set the HTTP header X-Trino-User')
    conn = None
    cur = None
    catalog = Unicode("", config=True, help='Trino catalog to use')
    schema = Unicode("", config=True, help='Trino schema to use')

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement to execute')
    @argument('-l', '--limit', metavar='max_rows', type=int, help='The maximum number of rows to display. A value of zero is equivalent to `--output skip`')
    @argument('-r', '--refresh', metavar='all|local|none', type=str, default='none',
                help='Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.')
    @argument('-d', '--dataframe', metavar='name', type=str, help='Capture results in pandas dataframe')
    @argument('-o', '--output', metavar='sql|json|html|grid|text|skip|none', type=str, default='html', help='Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)')
    @argument('-s', '--show-nonprinting', action='store_true', help='Replace none printable characters with their ascii codes (LF -> \x0a)')
    @argument('-x', '--raw', action='store_true', help="Run statement as is. Do not wrap statement with a limit. Use this option to run statement which can't be wrapped in a SELECT/LIMIT statement. For example EXPLAIN, SHOW TABLE, SHOW CATALOGS.")
    @argument('-c', '--catalog', metavar='catalogname', default=None, type=str, help='Trino catalog to use')
    @argument('-m', '--schema', metavar='schemaname', type=str, help='Trino schema to use')
    def trino(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %trino and as %%trino"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.trino, line)
        output_file = self.outputFile or f"{os.path.expanduser('~')}/.local/trinodb.schema.json"

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
            http_scheme=self.httpScheme)
        self.cur = self.conn.cursor()

        catalog_array = self.get_catalog_array()
        if self.should_update_schema(output_file, self.cacheTTL):
            update_database_schema(self.cur, output_file, catalog_array)
        else:
            if args.refresh.lower() == 'all':
                update_database_schema(self.cur, output_file, catalog_array)
                return
            elif args.refresh.lower() != 'none':
                print(f'Invalid refresh option given {args.refresh}. Valid refresh options are [all|local|none]')

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return

        limit = args.limit
        if limit is None:
            limit = self.limit

        if args.output.lower() == 'sql':
            return self.display_sql(sql)
        elif args.output.lower() == 'json':
            # Determine the resulting column names
            self.cur.execute(f'SHOW STATS FOR ({sql})')
            results = self.cur.fetchmany(limit+1)
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
        elif not args.raw == True:
            sql = f'select * from ({sql}) limit {limit+1}'
        self.cur.execute(sql)
        results = self.cur.fetchmany(limit+1)

        columns = list(map(lambda d: d[0], self.cur.description))

        if args.dataframe:
            print(f'Saved results to pandas dataframe named `{args.dataframe}`')
            pdf = pd.DataFrame.from_records(results, columns=columns)
            self.shell.user_ns.update({args.dataframe: pdf})

        if limit <= 0 or args.output.lower() == 'skip' or args.output.lower() == 'none':
            print('Query execution skipped')
            return
        elif args.output.lower() == 'grid':
            if len(results) > limit:
                print(f'Only showing top {limit} row(s)')
            pdf = pd.DataFrame.from_records(results, columns=columns)
            if args.show_nonprinting:
                for c in pdf.columns:
                    pdf[c] = pdf[c].apply(lambda v: escape_control_chars(str(v)))
            return render_grid(pdf, limit)
        elif args.output.lower() == 'json':
            if len(results) > limit:
                print('Only showing top %d row(s)' % limit)
            json_array = []
            for row in results[:limit]:
                python_obj = {}
                for idx, column_name in enumerate(columns):
                    python_val = None
                    if row[idx]:
                        python_val = json.loads(row[idx])
                    python_obj[column_name] = python_val
                json_array.append(python_obj)
            if args.show_nonprinting:
                recursive_escape(json_array)
            return JSON(json_array)
        elif args.output.lower() == 'html':
            if len(results) > limit:
                print(f'Only showing top {limit} row(s)')
            html = make_tag('tr', False,
                        ''.join(map(lambda x: make_tag('td', args.show_nonprinting, x, style='font-weight: bold'), columns)),
                        style='border-bottom: 1px solid')
            for index, row in enumerate(results[:limit]):
                html += make_tag('tr', False, ''.join(map(lambda x: make_tag('td', args.show_nonprinting, x),row)))
            return HTML(make_tag('table', False, html))
        elif args.output.lower() == 'text':
            if len(results) > limit:
                print(f'Only showing top {limit} row(s)')

            rows = results
            sb = ''
            numCols = len(columns)
            # We set a minimum column width at '3'
            minimumColWidth = 3

            # Initialise the width of each column to a minimum value
            colWidths = [minimumColWidth for i in range(numCols)]

            # Compute the width of each column
            for i, c in enumerate(columns):
                colWidths[i] = max(colWidths[i], len(c))
            for row in rows:
                for i, cell in enumerate(row):
                    colWidths[i] = max(colWidths[i], len(str(cell)))

            paddedColumns = []
            for i, c in enumerate(columns):
                newName = c.ljust(colWidths[i], ' ')
                paddedColumns.append(newName)

            paddedRows = []
            for row in rows:
                newRow = []
                for i, cell in enumerate(row):
                    v = str(cell)
                    newVal = v.ljust(colWidths[i], ' ')
                    newRow.append(newVal)
                paddedRows.append(newRow)

            # Create SeparateLine
            sep = "+"
            for n in colWidths:
                for i in range(n):
                    sep += "-"
                sep += "+"
            sep += "\n"

            sb = sep            
            sb += "|"
            for c in paddedColumns:
                sb += c + "|"
            sb += "\n"

            # data
            sb += sep
            for row in paddedRows:
                sb += "|"
                for cell in row:
                    sb += cell + "|"
                sb += "\n"
            sb += sep
            print(sb)
            return
        else:
            print(f'Invalid output option {args.output}. The valid options are [sql|json|html|grid|none].')
