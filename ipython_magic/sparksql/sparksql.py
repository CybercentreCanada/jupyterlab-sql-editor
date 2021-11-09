import os
import re
from html import escape

from IPython.core.display import HTML
from IPython.core.magic import Magics, line_cell_magic, line_magic, cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from pyspark.sql import SparkSession
from traitlets import Int, Unicode, Bool
from .schema_export import updateDatabaseSchema, updateLocalDatabase
from ..common.base import Base

@magics_class
class SparkSql(Base):

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument('sql', nargs='*', type=str, help='SQL statement')
    @argument('-l', '--limit', type=int, help='The maximum number of rows to display')
    @argument('-r', '--refresh', action='store_true', help=f'Force the regeneration of the schema cache file')
    @argument('-i', '--interactive', action='store_true', help='Display results in interactive grid')
    @argument('-p', '--print', action='store_true', type=str, help='Print SQL statement that will be executed (useful to test jinja templated statements')
    @argument('-d', '--dataframe', type=str, help='Capture dataframe in a local variable')
    @argument('-c', '--cache', action='store_true', help='Cache dataframe')
    @argument('-e', '--eager', action='store_true', help='Cache dataframe with eager load')
    @argument('-v', '--view', type=str, help='Create or replace temporary view')
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.sparksql, line)
        outputFile = self.outputFile or '/tmp/sparkdb.schema.json'

        spark = self.get_instantiated_spark_session()
        if spark is None:
            print("active spark session is not found")
            return

        catalog_array = self.get_catalog_array()
        if self.shouldUpdateSchema(spark, outputFile, self.cacheTTL, catalog_array):
            updateDatabaseSchema(spark, outputFile, catalog_array)
        if args.refresh:
            updateDatabaseSchema(spark, outputFile, catalog_array)
            return

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return
        elif args.print:
            return sql

        df = spark.sql(sql)
        if args.cache or args.eager:
            print('cache dataframe with %s load' % ('eager' if args.eager else 'lazy'))
            df = df.cache()
            if args.eager:
                df.count()
        if args.view:
            print('create temporary view `%s`' % args.view)
            df.createOrReplaceTempView(args.view)
            updateLocalDatabase(spark, outputFile)
        if args.dataframe:
            print('capture dataframe to local variable `%s`' % args.dataframe)
            self.shell.user_ns.update({args.dataframe: df})

        limit = args.limit or self.limit
        interactive = args.interactive or self.interactive

        if interactive:
            # It's important to import DataGrid inside this magic function
            # If you import it at the top of the file it will interfere with
            # the use of DataGrid in a notebook cell. You get a message
            # Loading widget...
            from ipydatagrid import DataGrid

            pdf = df.limit(limit + 1).toPandas()
            num_rows = pdf.shape[0]
            if num_rows > 0: 
                if num_rows > limit:
                    print('only showing top %d row(s)' % limit)
                    # Delete last row
                    pdf = pdf.head(num_rows -1) 
                return DataGrid(pdf, selection_mode="row", layout={"height": "1000px"})

            else:
                print('No results')
                return 
        else:
            header, contents = self.get_results(df, limit)
            if len(contents) > limit:
                print('only showing top %d row(s)' % limit)

            html = self.make_tag('tr',
                        ''.join(map(lambda x: self.make_tag('td', escape(x), style='font-weight: bold'), header)),
                        style='border-bottom: 1px solid')
            for index, row in enumerate(contents[:limit]):
                html += self.make_tag('tr', ''.join(map(lambda x: self.make_tag('td', escape(x)), row)))
            return HTML(self.make_tag('table', html)) 

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
