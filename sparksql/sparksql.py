import re
from html import escape

from IPython.core.display import HTML
from IPython.core.magic import Magics, line_cell_magic, line_magic, cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from pyspark.sql import SparkSession
from traitlets import Int, Unicode, Bool
from .schema_export import checkAndUpdateSchema

BIND_VARIABLE_PATTERN = re.compile(r'{([A-Za-z0-9_]+)}')

DEFAULT_SCHEMA_OUTFILE = '/tmp/sparkdb.schema.json'
DEFAULT_SCHEMA_TTL = 3600
DEFAULT_CATALOGS = 'default'
@magics_class
class SparkSql(Magics):
    limit = Int(20, config=True, help='The maximum number of rows to display')
    outputFile = Unicode(DEFAULT_SCHEMA_OUTFILE, config=True, help=f'Output schema to specified file, defaults to {DEFAULT_SCHEMA_OUTFILE}')
    cacheTTL = Int(DEFAULT_SCHEMA_TTL, config=True, help=f'Re-generate output schema file if older than time specified (defaults to {DEFAULT_SCHEMA_TTL} seconds)')
    catalogs = Unicode(DEFAULT_CATALOGS, config=True, help=f'Retrive schema from the specified list of catalogs (defaults to "{DEFAULT_CATALOGS}")')
    qgrid = Bool(False, config=True, help=f'Display results in qgrid')

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement')
    @argument('-d', '--dataframe', type=str, help='Capture dataframe in a local variable')
    @argument('-c', '--cache', action='store_true', help='Cache dataframe')
    @argument('-e', '--eager', action='store_true', help='Cache dataframe with eager load')
    @argument('-v', '--view', type=str, help='Create or replace temporary view')
    @argument('-l', '--limit', type=int, help='The maximum number of rows to display')
    @argument('-f', '--outputFile', type=str, help=f'Output schema to specified file, defaults to {DEFAULT_SCHEMA_OUTFILE}')
    @argument('-t', '--cacheTTL', type=int, help=f'Re-generate output schema file if older than time specified (defaults to {DEFAULT_SCHEMA_TTL} seconds)')
    @argument('-a', '--catalogs', type=str, help='Retrive schema from the specified list of catalogs')
    @argument('-q', '--qgrid', action='store_true', help='Display results in qgrid')
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"

        if local_ns is None:
            local_ns = {}

        user_ns = self.shell.user_ns.copy()
        user_ns.update(local_ns)

        args = parse_argstring(self.sparksql, line)

        spark = get_instantiated_spark_session()

        if spark is None:
            print("active spark session is not found")
            return

        outputFile = args.outputFile or self.outputFile
        cacheTTL = args.cacheTTL or self.cacheTTL
        catalogs = args.catalogs or self.catalogs
        checkAndUpdateSchema(spark, outputFile, cacheTTL, catalogs.split(','))

        sql = cell
        if cell is None:
            sql = ' '.join(args.sql)

        if not sql:
            print('No sql statement to execute')
            return

        df = spark.sql(bind_variables(sql, user_ns))
        if args.cache or args.eager:
            print('cache dataframe with %s load' % ('eager' if args.eager else 'lazy'))
            df = df.cache()
            if args.eager:
                df.count()
        if args.view:
            print('create temporary view `%s`' % args.view)
            df.createOrReplaceTempView(args.view)
        if args.dataframe:
            print('capture dataframe to local variable `%s`' % args.dataframe)
            self.shell.user_ns.update({args.dataframe: df})

        limit = args.limit or self.limit
        qgrid = args.qgrid or self.qgrid

        if qgrid:
            import qgrid
            pdf = df.limit(limit + 1).toPandas()
            num_rows = pdf.shape[0]
            if num_rows > 0: 
                if num_rows > limit:
                    print('only showing top %d row(s)' % limit)
                    # Delete last row
                    pdf = pdf.head(num_rows -1) 
                return qgrid.show_grid(pdf, show_toolbar=False, grid_options={'forceFitColumns': False})
            else:
                print('No results')
                return 
        else:
            header, contents = get_results(df, limit)
            if len(contents) > limit:
                print('only showing top %d row(s)' % limit)

            html = make_tag('tr',
                        ''.join(map(lambda x: make_tag('td', escape(x), style='font-weight: bold'), header)),
                        style='border-bottom: 1px solid')
            for index, row in enumerate(contents[:limit]):
                html += make_tag('tr', ''.join(map(lambda x: make_tag('td', escape(x)), row)))
            return HTML(make_tag('table', html)) 


def bind_variables(query, user_ns):
    def fetch_variable(match):
        variable = match.group(1)
        if variable not in user_ns:
            raise NameError('variable `%s` is not defined', variable)
        return str(user_ns[variable])

    return re.sub(BIND_VARIABLE_PATTERN, fetch_variable, query)


def get_results(df, limit):
    def convert_value(value):
        if value is None:
            return 'null'
        return str(value)

    header = df.columns
    contents = list(map(lambda row: list(map(convert_value, row)), df.take(limit + 1)))

    return header, contents


def make_tag(tag_name, body='', **kwargs):
    attributes = ' '.join(map(lambda x: '%s="%s"' % x, kwargs.items()))
    if attributes:
        return '<%s %s>%s</%s>' % (tag_name, attributes, body, tag_name)
    else:
        return '<%s>%s</%s>' % (tag_name, body, tag_name)


def get_instantiated_spark_session():
    return SparkSession._instantiatedSession
