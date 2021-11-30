import os
import re
from html import escape
import pandas as pd
import numpy
import math


import trino
from IPython.core.display import HTML
from IPython.core.magic import Magics, line_cell_magic, line_magic, cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from traitlets import Int, Unicode, Instance
from .schema_export import updateDatabaseSchema
from ..common.base import Base

@magics_class
class Trino(Base):
    host = Unicode('localhost', config=True, help='The trino server hostname)')
    port = Int(443, config=True, help='Trino server port number)')
    httpScheme = Unicode('https', config=True, help='Trino server scheme https/http)')
    auth = Instance(allow_none=True, klass='trino.auth.Authentication', config=True, help='An instance of the Trino Authentication class')    
    user = Unicode('user', config=True, help='Trino user to use when no authentication is specified. This will set the HTTP header X-Trino-User)')
    conn = None
    cur = None

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement to execute')
    @argument('-l', '--limit', metavar='max_rows', type=int, help='The maximum number of rows to display. A value of zero is equivalent to `--output skip`')
    @argument('-r', '--refresh', metavar='all|local|none', type=str, default='none', help='Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.')
    @argument('-d', '--dataframe', metavar='name', type=str, help='Capture results in pandas dataframe')
    @argument('-o', '--output', metavar='sql|json|html|grid|skip|none', type=str, default='html', help='Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)')
    def trino(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %trino and as %%trino"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.trino, line)
        outputFile = self.outputFile or '/tmp/trinodb.schema.json'

        if not self.conn:
            self.conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                auth=self.auth,
                user=self.user,
                http_scheme=self.httpScheme)
            self.cur = self.conn.cursor()

        catalog_array = self.get_catalog_array()
        if self.shouldUpdateSchema(self.cur, outputFile, self.cacheTTL, catalog_array):
            updateDatabaseSchema(self.cur, outputFile, catalog_array)

        if args.refresh.lower() == 'all':
            updateDatabaseSchema(self.cur, outputFile, catalog_array)
            return
        elif args.refresh.lower() != 'none':
            print(f'Invalid refresh option given {args.refresh}. Valid refresh options are [all|local|none]')

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return

        limit = args.limit
        if limit == None:
            limit = self.limit

        sql = f'select * from ({sql}) limit {limit+1}'
        if args.output.lower() == 'sql':
            return self.display_sql(sql)
        elif args.output.lower() == 'json':
            sql = f'select cast(row(*) as JSON) as json_str from ({sql})'
        
        self.cur.execute(sql)
        results = self.cur.fetchmany(limit+1)

        columns = list(map(lambda d: d[0], self.cur.description))

        if args.dataframe:
            print('Saved results to pandas dataframe named `%s`' % args.dataframe)
            pdf = pd.DataFrame.from_records(results, columns=columns)
            self.shell.user_ns.update({args.dataframe: pdf})

        if limit <= 0 or args.output.lower() == 'skip' or args.output.lower() == 'none':
            print('Query execution skipped')
            return
        elif args.output.lower() == 'grid':
            if len(results) > limit:
                print('Only showing top %d row(s)' % limit)
            pdf = pd.DataFrame.from_records(results, columns=columns)
            return self.render_grid(pdf, limit)
        elif args.output.lower() == 'json':
            print('Not implemented yet')
            return
        elif args.output.lower() == 'html':
            if len(results) > limit:
                print('Only showing top %d row(s)' % limit)
            html = self.make_tag('tr',
                            ''.join(map(lambda x: self.make_tag('td', escape(str(x)), style='font-weight: bold'), columns)),
                            style='border-bottom: 1px solid')
            for index, row in enumerate(results):
                html += self.make_tag('tr', ''.join(map(lambda x: self.make_tag('td', escape(str(x))), row)))

            return HTML(self.make_tag('table', html))
        else:
            print(f'Invalid output option {args.output}. The valid options are [sql|json|html|grid|none].')





