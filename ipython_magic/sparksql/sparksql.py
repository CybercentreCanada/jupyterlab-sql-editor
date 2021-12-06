import json

from IPython.core.display import HTML, JSON
from IPython.core.magic import line_cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from .schema_export import update_database_schema, update_local_database
from ..common.base import Base

@magics_class
class SparkSql(Base):

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement to execute')
    @argument('-l', '--limit', metavar='max_rows', type=int, help='The maximum number of rows to display. A value of zero is equivalent to `--output skip`')
    @argument('-r', '--refresh', metavar='all|local|none', type=str, default='none',
                help='Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.')
    @argument('-d', '--dataframe', metavar='name', type=str, help='Capture dataframe in a local variable named `name`')
    @argument('-c', '--cache', action='store_true', help='Cache dataframe')
    @argument('-e', '--eager', action='store_true', help='Cache dataframe with eager load')
    @argument('-v', '--view', metavar='name', type=str, help='Create or replace a temporary view named `name`')
    @argument('-o', '--output', metavar='sql|json|html|grid|skip|none', type=str, default='html',
                help='Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)')
    @argument('-s', '--show-nonprinting', action='store_true', help='Replace none printable characters with their ascii codes (LF -> \x0a)')
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.sparksql, line)
        output_file = self.outputFile or '/tmp/sparkdb.schema.json'

        spark = self.get_instantiated_spark_session()
        if spark is None:
            print("Active spark session is not found")
            return

        catalog_array = self.get_catalog_array()
        if self.should_update_schema(output_file, self.cacheTTL):
            update_database_schema(spark, output_file, catalog_array)

        if args.refresh.lower() == 'all':
            update_database_schema(spark, output_file, catalog_array)
            update_local_database(spark, output_file)
            return
        elif args.refresh.lower() == 'local':
            update_local_database(spark, output_file)
        elif args.refresh.lower() != 'none':
            print(f'Invalid refresh option given {args.refresh}. Valid refresh options are [all|local|none]')

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return
        elif args.output.lower() == 'sql':
            return self.display_sql(sql)

        df = spark.sql(sql)
        if args.cache or args.eager:
            load_type = 'eager' if args.eager else 'lazy'
            print(f'Cached dataframe with {load_type} load')
            df = df.cache()
            if args.eager:
                df.count()
        if args.view:
            print(f'Created temporary view `{args.view}`')
            df.createOrReplaceTempView(args.view)
            update_local_database(spark, output_file)
        if args.dataframe:
            print(f'Captured dataframe to local variable `{args.dataframe}`')
            self.shell.user_ns.update({args.dataframe: df})

        limit = args.limit
        if limit is None:
            limit = self.limit

        if limit <= 0 or args.output.lower() == 'skip' or args.output.lower() == 'none':
            print('Query execution skipped')
            return
        elif args.output.lower() == 'grid':
            pdf = df.limit(limit + 1).toPandas()
            if args.show_nonprinting:
                for column in pdf.columns:
                    pdf[column] = pdf[column].apply(lambda v: self.escape_control_chars(str(v)))
            num_rows = pdf.shape[0]
            if num_rows > limit:
                print(f'Only showing top {limit} row(s)')
                # Delete last row
                pdf = pdf.head(num_rows -1)
            return self.render_grid(pdf, limit)
        elif args.output.lower() == 'json':
            results = df.select(F.to_json(F.struct(F.col("*"))).alias("json_str")).take(limit)
            json_array = [json.loads(r.json_str) for r in results]
            if args.show_nonprinting:
                self.recursive_escape(json_array)
            return JSON(json_array)
        elif args.output.lower() == 'html':
            header, contents = self.get_results(df, limit)
            if len(contents) > limit:
                print(f'Only showing top {limit} row(s)')
            html = self.make_tag('tr', False,
                        ''.join(map(lambda x: self.make_tag('td', args.show_nonprinting, x, style='font-weight: bold'), header)),
                        style='border-bottom: 1px solid')
            for index, row in enumerate(contents[:limit]):
                html += self.make_tag('tr', False, ''.join(map(lambda x: self.make_tag('td', args.show_nonprinting, x),row)))
            return HTML(self.make_tag('table', False, html))
        else:
            print(f'Invalid output option {args.output}. The valid options are [sql|json|html|grid|none].')

    @staticmethod
    def get_results(df, limit):
        def convert_value(value):
            if value is None:
                return 'null'
            return str(value)

        header = df.columns
        contents = list(map(lambda row: list(map(convert_value, row)), df.take(limit + 1)))

        return header, contents

    @staticmethod
    def get_instantiated_spark_session():
        return SparkSession._instantiatedSession
