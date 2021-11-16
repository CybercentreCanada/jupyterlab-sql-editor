import os
import re
from html import escape
import math

from IPython.core.display import HTML, JSON
from IPython.core.magic import Magics, line_cell_magic, line_magic, cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from traitlets import Int, Unicode, Bool
import json
from .schema_export import updateDatabaseSchema, updateLocalDatabase
from ..common.base import Base

@magics_class
class SparkSql(Base):

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement to execute')
    @argument('-l', '--limit', metavar='max_rows', type=int, help='The maximum number of rows to display. A value of zero is equivalent to `--output skip`')
    @argument('-r', '--refresh', metavar='all|local|none', type=str, default='none', help='Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.')
    @argument('-d', '--dataframe', metavar='name', type=str, help='Capture dataframe in a local variable named `name`')
    @argument('-c', '--cache', action='store_true', help='Cache dataframe')
    @argument('-e', '--eager', action='store_true', help='Cache dataframe with eager load')
    @argument('-v', '--view', metavar='name', type=str, help='Create or replace a temporary view named `name`')
    @argument('-o', '--output', metavar='sql|json|html|grid|skip|none', type=str, default='html', help='Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)')
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.sparksql, line)
        outputFile = self.outputFile or '/tmp/sparkdb.schema.json'

        spark = self.get_instantiated_spark_session()
        if spark is None:
            print("Active spark session is not found")
            return

        catalog_array = self.get_catalog_array()
        if self.shouldUpdateSchema(spark, outputFile, self.cacheTTL, catalog_array):
            updateDatabaseSchema(spark, outputFile, catalog_array)

        if args.refresh.lower() == 'all':
            updateDatabaseSchema(spark, outputFile, catalog_array)
            updateLocalDatabase(spark, outputFile)
            return
        elif args.refresh.lower() == 'local':
            updateLocalDatabase(spark, outputFile)
        elif args.refresh.lower() != 'none':
            print(f'Invalid refresh option given {args.refresh}. Valid refresh options are [all|local|none]')

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return
        elif args.output.lower() == 'sql':
            return sql

        df = spark.sql(sql)
        if args.cache or args.eager:
            print('Cached dataframe with %s load' % ('eager' if args.eager else 'lazy'))
            df = df.cache()
            if args.eager:
                df.count()
        if args.view:
            print('Created temporary view `%s`' % args.view)
            df.createOrReplaceTempView(args.view)
            updateLocalDatabase(spark, outputFile)
        if args.dataframe:
            print('Captured dataframe to local variable `%s`' % args.dataframe)
            self.shell.user_ns.update({args.dataframe: df})


        limit = args.limit
        if limit == None:
            limit = self.limit
        if limit <= 0 or args.output.lower() == 'skip' or args.output.lower() == 'none':
            print('Query execution skipped')
            return
        elif args.output.lower() == 'grid':
            # It's important to import DataGrid inside this magic function
            # If you import it at the top of the file it will interfere with
            # the use of DataGrid in a notebook cell. You get a message
            # Loading widget...
            from ipydatagrid import DataGrid

            pdf = df.limit(limit + 1).toPandas()
            num_rows = pdf.shape[0]
            if num_rows > 0: 
                if num_rows > limit:
                    print('Only showing top %d row(s)' % limit)
                    # Delete last row
                    pdf = pdf.head(num_rows -1)

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
            else:
                print('No results')
                return 
        elif args.output.lower() == 'json':
            results = df.select(F.to_json(F.struct(F.col("*"))).alias("json_str")).take(limit)
            json_array = [json.loads(r.json_str) for r in results]
            return JSON(json_array)
        elif args.output.lower() == 'html':
            header, contents = self.get_results(df, limit)
            if len(contents) > limit:
                print('Only showing top %d row(s)' % limit)

            html = self.make_tag('tr',
                        ''.join(map(lambda x: self.make_tag('td', escape(x), style='font-weight: bold'), header)),
                        style='border-bottom: 1px solid')
            for index, row in enumerate(contents[:limit]):
                html += self.make_tag('tr', ''.join(map(lambda x: self.make_tag('td', escape(x)), row)))
            return HTML(self.make_tag('table', html)) 
        else:
            print(f'Invalid output option {args.output}. The valid options are [sql|json|html|grid|none].')

    def get_results(self, df, limit):
        def convert_value(value):
            if value is None:
                return 'null'
            return str(value)

        header = df.columns
        contents = list(map(lambda row: list(map(convert_value, row)), df.take(limit + 1)))

        return header, contents


    def get_instantiated_spark_session(self):
        return SparkSession._instantiatedSession
