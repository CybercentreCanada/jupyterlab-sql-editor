import logging
import os
import secrets
from importlib import reload
from pathlib import Path
from time import time

try:
    import dbt.events
    import dbt.main
except ImportError:
    pass
# Handling Python 3.11 incompatibility with older dbt versions
except ValueError:
    pass

import pandas as pd
import sqlglot
from IPython.core.magic import (
    line_cell_magic,
    line_magic,
    magics_class,
    needs_local_scope,
)
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import Pretty
from pyspark.errors.exceptions.captured import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    QueryExecutionException,
    StreamingQueryException,
)
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from traitlets import List, Unicode

from jupyterlab_sql_editor.ipython_magic.base import Base
from jupyterlab_sql_editor.ipython_magic.result_cache import ResultCache
from jupyterlab_sql_editor.ipython_magic.sparksql.spark_export import update_database_schema, update_local_database
from jupyterlab_sql_editor.ipython_magic.sparksql.sparkdf import display_df
from jupyterlab_sql_editor.outputters.outputters import _display_results
from jupyterlab_sql_editor.outputters.util import to_pandas

VALID_OUTPUTS = ["sql", "text", "json", "html", "aggrid", "grid", "schema", "skip", "none"]
PYSPARK_ERROR_TYPES = (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    QueryExecutionException,
    StreamingQueryException,
)


@magics_class
class SparkSql(Base):
    dbt_args = List([], config=True, help="dbt arguments")
    dbt_project_dir = Unicode("", config=True, help="Path to dbt project directory")

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.spark = None
        self.cached_results = ResultCache()

    @needs_local_scope
    @line_cell_magic
    @magic_arguments()
    @argument("sql", nargs="*", type=str, help="SQL statement to execute")
    @argument("--transpile", metavar="transpile", type=str, help="Transpile query to target dialect")
    @argument(
        "-l",
        "--limit",
        metavar="max_rows",
        type=int,
        help="The maximum number of rows to display. A value of zero is equivalent to `--output skip`",
    )
    @argument(
        "-r",
        "--refresh",
        metavar="all|local|none",
        type=str,
        default="none",
        help="Force the regeneration of the schema cache file. The `local` option will only update tables/views created in the local Spark context.",
    )
    @argument("-d", "--dataframe", metavar="name", type=str, help="Capture dataframe in a local variable named `name`")
    @argument("-c", "--cache", action="store_true", help="Cache dataframe")
    @argument("-e", "--eager", action="store_true", help="Cache dataframe with eager load")
    @argument("-v", "--view", metavar="name", type=str, help="Create or replace a temporary view named `name`")
    @argument("-i", "--input", metavar="name", type=str, help="Display pandas or Spark dataframe")
    @argument(
        "-o",
        "--output",
        metavar="sql|json|html|aggrid|grid|text|schema|skip|none",
        type=str,
        default="html",
        help="Output format. Defaults to html. The `sql` option prints the SQL statement that will be executed (useful to test jinja templated statements)",
    )
    @argument(
        "-s",
        "--show-nonprinting",
        action="store_true",
        help="Replace none printable characters with their ascii codes (LF -> \x0a)",
    )
    @argument("-j", "--jinja", action="store_true", help="Enable Jinja templating support")
    @argument("-b", "--dbt", action="store_true", help="Enable DBT templating support")
    @argument("--database", metavar="databasename", default=None, type=str, help="Spark database to use")
    @argument("-t", "--truncate", metavar="max_cell_length", type=int, help="Truncate output")
    @argument(
        "-m",
        "--streaming_mode",
        metavar="update|complete",
        type=str,
        default="update",
        help="The mode of streaming queries.",
    )
    @argument(
        "-x",
        "--lean-exceptions",
        action="store_true",
        help="Shortened exceptions. Might be helpful if the exceptions reported by Spark are noisy such as with big SQL queries",
    )
    @argument("--expand", action="store_true", help="Expand json results")
    @argument(
        "-h",
        "--help",
        action="store_true",
        help="Detailed information about SparkSQL magic",
    )
    @argument("--jsonnulls", action="store_true", help="Show nulls for JSON output")
    def sparksql(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %sparksql and as %%sparksql"
        self.set_user_ns(local_ns)
        args = parse_argstring(self.sparksql, line)
        limit, truncate, output = self.parse_common_args(args, self.limit)
        streaming_mode = args.streaming_mode.lower()
        use_cache = False

        # Equivalent to %sparksql? or %%sparksql?
        if args.help:
            return self.show_help("sparksql")

        if args.transpile:
            return Pretty(
                sqlglot.transpile(
                    sql=self.resolve_sql(cell, args.sql, args.dbt, args.jinja),
                    read="spark",
                    write=args.transpile,
                    pretty=True,
                )[0]
            )

        if output not in VALID_OUTPUTS:
            print(f"Invalid output option {args.output}. The valid options are {VALID_OUTPUTS}.")
            return

        if args.input and isinstance(self.get_shell().user_ns.get(args.input), pd.DataFrame) and cell is None:
            _display_results(
                pdf=self.get_shell().user_ns.get(args.input) or pd.DataFrame([]),
                result_id=secrets.token_hex(32),
                output=output,
                show_nonprinting=args.show_nonprinting,
                truncate=truncate,
                args=args,
            )
            return

        self.spark = self.session()

        if args.database:
            self.spark.sql(f"USE {args.database}")

        if self.check_refresh(refresh_arg=args.refresh.lower(), cache_ttl=self.cacheTTL):
            return

        # If --input exists and is a Spark dataframe, take it as it is otherwise create dataframe from sql statement
        if args.input and cell is None:
            sql = ""
            result_id = secrets.token_hex(32)
            df = self.resolve_input_dataframe(args.input, cell, (pd.DataFrame, DataFrame))
        else:
            sql = self.resolve_sql(cell, args.sql, dbt=args.dbt, jinja=args.jinja)
            result_id = self.sql_hash(sql, limit)

            if output == "sql":
                return self.display_sql(sql)

            # try-except here as well because it can also raise PYSPARK_ERROR_TYPES
            try:
                df = self.spark.sql(sql)
            except PYSPARK_ERROR_TYPES as exc:
                if args.lean_exceptions:
                    self.print_pyspark_error(exc)
                    return
                else:
                    raise exc

        if args.cache or args.eager:
            load_type = "eager" if args.eager else "lazy"
            print(f"Cached dataframe with {load_type} load")
            df = df.cache()
            if args.eager:
                df.count()

        if args.dataframe:
            print(f"Captured dataframe to local variable `{args.dataframe}`")
            self.get_shell().user_ns.update({args.dataframe: df})

        if output == "schema":
            df.printSchema()
            return

        if not (output == "skip" or output == "schema" or output == "none"):
            try:
                start = time()
                results = self.spark.createDataFrame(df.take(limit + 1), schema=df.schema)
                end = time()
                pdf = to_pandas(results, self.spark._jconf)
                self.print_execution_stats(end, start, len(pdf), limit)
            except PYSPARK_ERROR_TYPES as exc:
                if args.lean_exceptions:
                    self.print_pyspark_error(exc)
                    return
                else:
                    raise exc
        else:
            results, pdf = None, None
            print("Display and execution of results skipped")

        if not use_cache:
            self.cached_results.put(result_id=result_id, results=pdf)

        display_df(
            original_df=df,
            df=results,
            pdf=pdf if pdf is not None else pd.DataFrame([]),
            result_id=result_id,
            limit=limit,
            output=output,
            truncate=truncate,
            show_nonprinting=args.show_nonprinting,
            query_name=args.view,
            sql=sql,
            streaming_mode=streaming_mode,
            args=args,
        )

    @property
    def default_schema_file(self):
        return "sparkdb.schema.json"

    def session(self, catalog=None, schema=None, host=None, source=None) -> SparkSession:
        """
        Returns the active SparkSession if it exists.
        Raises RuntimeError if no SparkSession has been instantiated.
        """
        session = SparkSession._instantiatedSession
        if session is None:
            raise RuntimeError("No active SparkSession found. Please start a Spark session first.")
        return session

    def update_schema(self, target: SparkSession, output_file: Path, catalog_array):
        update_database_schema(target, output_file, catalog_array)

    def update_local_schema(self, target: SparkSession, output_file: Path, catalog_array):
        update_local_database(target, output_file, catalog_array)

    def resolve_sql(self, cell: str | None, sql_args: list[str], dbt: bool, jinja: bool) -> str:
        if dbt:
            sql = self.get_dbt_sql_statement(cell, sql_args)
        else:
            sql = self.get_sql_statement(cell, sql_args, jinja)

        if not sql or not sql.strip():
            raise ValueError("No valid SQL statement provided.")

        return sql

    @staticmethod
    def print_pyspark_error(exc) -> None:
        if isinstance(exc, AnalysisException):
            print(f"AnalysisException: {exc.desc.splitlines()[0]}")
        elif isinstance(exc, ParseException):
            print(f"ParseException: {exc.desc}")
        elif isinstance(exc, StreamingQueryException):
            print(f"StreamingQueryException: {exc.cause.desc if exc.cause else exc.desc}")
        elif isinstance(exc, QueryExecutionException):
            print(f"QueryExecutionException: {exc.desc}")
        elif isinstance(exc, IllegalArgumentException):
            print(f"IllegalArgumentException: {exc.desc}")

    def get_dbt_sql_statement(self, cell, sql_argument):
        sql = cell
        if cell is None:
            sql = " ".join(sql_argument)
        if not sql:
            print("No sql statement to execute")
            return None

        stage_file_path = f"{self.dbt_project_dir}/analyses/__sparksql__stage_file__.sql"
        with open(stage_file_path, "w", encoding="utf8") as stage_file:
            stage_file.write(sql)

        dbt_compile_args = [
            "--no-write-json",
            "compile",
            "--model",
            "__sparksql__stage_file__",
        ] + self.dbt_args
        results, succeeded = self.invoke_dbt(dbt_compile_args)
        os.remove(stage_file_path)
        if succeeded and results:
            compiled_file_path = f"{self.dbt_project_dir}/{results.results[0].node.compiled_path}"
            with open(compiled_file_path, "r", encoding="utf8") as compiled_file:
                compiled_sql = compiled_file.read()
            return compiled_sql
        return ""

    @staticmethod
    def import_dbt():
        # reset dbt logging to prevent duplicate log entries.
        try:
            reload(dbt.main)
            reload(dbt.events)
        except Exception:
            print("Error reloading dbt modules")
            return False
        logger = logging.getLogger("configured_std_out")
        while logger.handlers:  # Only remove explicitly attached handlers
            logger.removeHandler(logger.handlers[0])
        return True

    def invoke_dbt(self, args):
        if self.import_dbt():
            return dbt.main.handle_and_check(args)
        return None, False

    def get_dbt_project_dir(self, args):
        if self.import_dbt():
            parsed = dbt.main.parse_args(args)
            return parsed.project_dir
        return None

    @line_magic
    def dbt(self, line=None):
        self.dbt_args = line.split() if line else []
        self.dbt_project_dir = self.get_dbt_project_dir(["debug"] + self.dbt_args)
        if not self.dbt_project_dir:
            print("dbt project directory not specified")
            return
        os.chdir(self.dbt_project_dir)
        self.invoke_dbt(["debug"] + self.dbt_args)

        # Exporting SparkSession to variable spark if created
        spark_session = self.session()
        if spark_session is not None:
            self.spark = spark_session
            print("Captured SparkSession to local variable `spark`")
            self.get_shell().user_ns.update({"spark": spark_session})
