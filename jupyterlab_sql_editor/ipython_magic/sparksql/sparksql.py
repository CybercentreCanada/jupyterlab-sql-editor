import os
import logging

import dbt.main
import dbt.logger

from importlib import reload, util
from IPython.core.magic import line_cell_magic, line_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from jupyterlab_sql_editor.ipython.sparkdf import display_df
from jupyterlab_sql_editor.ipython_magic.common.base import Base
from jupyterlab_sql_editor.ipython_magic.sparksql.spark_export import update_database_schema, update_local_database
from pyspark.sql import SparkSession
from time import time
from traitlets import List, Unicode

VALID_OUTPUTS = ['sql', 'text', 'json', 'html', 'grid', 'schema', 'skip', 'none']


@magics_class
class SparkSql(Base):
    dbt_args = List([], config=True, help='dbt arguments')
    dbt_project_dir = Unicode('', config=True, help='Path to dbt project directory')

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.spark = None

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
    @argument('-j', '--jinja', action='store_true', help='Enable Jinja templating support')
    @argument('-b', '--dbt', action='store_true', help='Enable DBT templating support')
    @argument('-t', '--truncate', metavar='max_cell_length', type=int, help='Truncate output')
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"
        self.set_user_ns(local_ns)
        args = parse_argstring(self.sparksql, line)
        output_file = self.outputFile or f"{os.path.expanduser('~')}/.local/sparkdb.schema.json"
        output = args.output.lower()

        truncate = 256
        if args.truncate and args.truncate > 0:
            truncate = args.truncate

        if not output in VALID_OUTPUTS:
            print(f'Invalid output option {args.output}. The valid options are [sql|json|text|html|grid|skip|none].')
            return

        self.spark = self.get_instantiated_spark_session()
        if self.spark is None:
            print("Active spark session is not found")
            return

        catalog_array = self.get_catalog_array()
        if self.check_refresh(args.refresh.lower(), output_file, catalog_array):
            return

        if args.dbt:
            sql = self.get_dbt_sql_statement(cell, args.sql)
        else:
            sql = self.get_sql_statement(cell, args.sql, args.jinja)

        if not sql:
            return
        elif output == 'sql':
            return self.display_sql(sql)

        # statements like INSERT INTO, USE SCHEMA, CREATE TABLE, DROP TABLE
        # execute immediatly, they do not require us to perform a .show(), .collect()
        # we detect these use case by checking for an empty list of columns (no return schema)
        # we treat these use cases differently than a SELECT that returns rows of data
        start = time()
        result = self.spark.sql(sql)
        end = time()
        if len(result.columns) == 0:
            elapsed = end - start
            print(f"Execution time: {elapsed:.2f} seconds")
            return

        if args.cache or args.eager:
            load_type = 'eager' if args.eager else 'lazy'
            print(f'Cached dataframe with {load_type} load')
            result = result.cache()
            if args.eager:
                result.count()
        if args.view:
            print(f'Created temporary view `{args.view}`')
            result.createOrReplaceTempView(args.view)
        if args.dataframe:
            print(f'Captured dataframe to local variable `{args.dataframe}`')
            self.shell.user_ns.update({args.dataframe: result})


        limit = args.limit
        if limit is None:
            limit = self.limit

        if limit <= 0 or output == 'skip' or output == 'none':
            print('Query execution skipped')
            return

        if output == 'schema':
            result.printSchema()
            return

        display_df(result, output=output, limit=limit, truncate=truncate, show_nonprinting=args.show_nonprinting, sql=sql)

    def check_refresh(self, refresh_arg, output_file, catalog_array):
        if refresh_arg == 'all':
            update_database_schema(self.spark, output_file, catalog_array)
            return True
        if refresh_arg == 'local':
            update_local_database(self.spark, output_file)
            return True
        if refresh_arg != 'none':
            print(f'Invalid refresh option given {refresh_arg}. Valid refresh options are [all|local|none]')
        return False

    @staticmethod
    def get_instantiated_spark_session():
        return SparkSession._instantiatedSession

    def get_dbt_sql_statement(self, cell, sql_argument):
        sql = cell
        if cell is None:
            sql = ' '.join(sql_argument)
        if not sql:
            print('No sql statement to execute')
            return None

        stage_file_path = self.dbt_project_dir + "/analyses/__sparksql__stage_file__.sql"
        with open(stage_file_path, "w", encoding="utf8") as stage_file:
            stage_file.write(sql)

        dbt_compile_args = [
            '--no-write-json',
            'compile',
            '--model',
            '__sparksql__stage_file__',
            ] + self.dbt_args
        results, succeeded = self.invoke_dbt(dbt_compile_args)
        os.remove(stage_file_path)
        if succeeded:
            compiled_file_path = self.dbt_project_dir + "/" + results.results[0].node.compiled_path
            with open(compiled_file_path, "r", encoding="utf8") as compiled_file:
                compiled_sql = compiled_file.read()
            return compiled_sql
        return ""

    @staticmethod
    def import_dbt():
        if not util.find_spec("dbt.main"):
            print('dbt is not installed\npip install dbt-core')
            return False

        # reset dbt logging to prevent duplicate log entries.
        reload(dbt.main)
        reload(dbt.logger)
        logger = logging.getLogger("configured_std_out")
        while logger.hasHandlers():
            logger.removeHandler(logger.handlers[0])
        return True

    def invoke_dbt(self, args):
        if self.import_dbt():
            return dbt.main.handle_and_check(args)
        return None

    def get_dbt_project_dir(self, args):
        if self.import_dbt():
            parsed = dbt.main.parse_args(args)
            return parsed.project_dir
        return None

    @line_magic
    def dbt(self, line=None):
        self.dbt_args = line.split()
        self.dbt_project_dir = self.get_dbt_project_dir(['debug'] + self.dbt_args)
        if not self.dbt_project_dir:
            print('dbt project directory not specified')
            return
        os.chdir(self.dbt_project_dir)
        self.invoke_dbt(['debug'] + self.dbt_args)
