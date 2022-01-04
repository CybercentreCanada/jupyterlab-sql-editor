import json
import os

from IPython.core.display import display, HTML, clear_output

from IPython.core.magic import line_cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from ipython_magic.common.base import Base
from ipython_magic.sparksql.spark_export import update_database_schema, update_local_database

from cccs_display.sparkdf import display_spark_df
from time import time, strftime, localtime

@magics_class
class SparkSql(Base):
    valid_outputs = ['sql','text','json','html','grid','schema','skip','none']

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
    @argument('-o', '--output', metavar='sql|json|html|grid|text|schema|skip|none', type=str, default='html',
                help='Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)')
    @argument('-s', '--show-nonprinting', action='store_true', help='Replace none printable characters with their ascii codes (LF -> \x0a)')
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"

        self.set_user_ns(local_ns)
        args = parse_argstring(self.sparksql, line)
        output_file = self.outputFile or f"{os.path.expanduser('~')}/.local/sparkdb.schema.json"
        output = args.output.lower()

        if not output in self.valid_outputs:
            print(f'Invalid output option {args.output}. The valid options are [sql|json|text|html|grid|skip|none].')
            return

        self.spark = self.get_instantiated_spark_session()
        if self.spark is None:
            print("Active spark session is not found")
            return

        catalog_array = self.get_catalog_array()
        if self.should_update_schema(output_file, self.cacheTTL):
            update_database_schema(self.spark, output_file, catalog_array)
        else:
            if args.refresh.lower() == 'all':
                update_database_schema(self.spark, output_file, catalog_array)
                return
            elif args.refresh.lower() == 'local':
                update_local_database(self.spark, output_file)
                return
            elif args.refresh.lower() != 'none':
                print(f'Invalid refresh option given {args.refresh}. Valid refresh options are [all|local|none]')

        sql = self.get_sql_statement(cell, args.sql)
        if not sql:
            return
        elif output == 'sql':
            return self.display_sql(sql)

        df = self.spark.sql(sql)
        if args.cache or args.eager:
            load_type = 'eager' if args.eager else 'lazy'
            print(f'Cached dataframe with {load_type} load')
            df = df.cache()
            if args.eager:
                df.count()
        if args.view:
            print(f'Created temporary view `{args.view}`')
            df.createOrReplaceTempView(args.view)
            update_local_database(self.spark, output_file)
        if args.dataframe:
            print(f'Captured dataframe to local variable `{args.dataframe}`')
            self.shell.user_ns.update({args.dataframe: df})

        limit = args.limit
        if limit is None:
            limit = self.limit

        if limit <= 0 or output == 'skip' or output == 'none':
            print('Query execution skipped')
            return

        if output == 'schema':
            df.printSchema()
            return

        self.display_link()
        displays = display_spark_df(df, output, limit, args.show_nonprinting)
        clear_output(wait=True)
        for d in displays:
            display(d)

    def display_link(self):
        link = self.spark._sc.uiWebUrl
        appName = self.spark._sc.appName
        applicationId = self.spark._sc.applicationId
        reverse_proxy = os.environ.get('SPARK_UI_URL')
        if reverse_proxy:
            link = f"{reverse_proxy}/proxy/{applicationId}"
        display(HTML(f"""<a class="external" href="{link}" target="_blank" >⭐ Spark {appName} UI 🡽</a>"""))


    @staticmethod
    def get_instantiated_spark_session():
        return SparkSession._instantiatedSession





