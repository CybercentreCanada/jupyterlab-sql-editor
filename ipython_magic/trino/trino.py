import os
import re
from html import escape

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
 
    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement')
    @argument('-l', '--limit', type=int, help='The maximum number of rows to display')
    @argument('-r', '--refresh', action='store_true', help=f'Force the regeneration of the schema cache file')
    @argument('-i', '--interactive', action='store_true', help='Display results in interactive grid')
    @argument('-p', '--print', action='store_true', help='Print SQL statement that will be executed (useful to test jinja templated statements')
    def trino(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %trino and as %%trino"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.trino, line)
        outputFile = self.outputFile or '/tmp/trinodb.schema.json'

        conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            auth=self.auth,
            user=self.user,
            http_scheme=self.httpScheme)
        cur = conn.cursor()

        catalog_array = self.get_catalog_array()
        if self.shouldUpdateSchema(cur, outputFile, self.cacheTTL, catalog_array):
            updateDatabaseSchema(cur, outputFile, catalog_array)
        if args.refresh:
            updateDatabaseSchema(cur, outputFile, catalog_array)
            return

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return
        elif args.print:
            return sql

        limit = args.limit or self.limit
        interactive = args.interactive or self.interactive
        cur.execute(sql)
        rows = cur.fetchmany(limit)

        header = list(map(lambda d: d[0], cur.description))
        
        if len(rows) > limit:
            print('only showing top %d row(s)' % limit)

        html = self.make_tag('tr',
                        ''.join(map(lambda x: self.make_tag('td', escape(str(x)), style='font-weight: bold'), header)),
                        style='border-bottom: 1px solid')
        for index, row in enumerate(rows):
            html += self.make_tag('tr', ''.join(map(lambda x: self.make_tag('td', escape(str(x))), row)))

        return HTML(self.make_tag('table', html))



