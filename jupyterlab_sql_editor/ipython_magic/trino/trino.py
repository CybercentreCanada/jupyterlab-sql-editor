import logging
import pathlib
from time import time
from typing import Optional

import pandas as pd
import sqlglot
import sqlparse
import trino
from IPython.core.magic import line_cell_magic, magics_class, needs_local_scope
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import Pretty, display
from sqlparse.sql import IdentifierList, TokenList
from sqlparse.tokens import Keyword
from traitlets import Bool, Instance, Int, Unicode, Union
from trino.sqlalchemy.datatype import parse_sqltype

from jupyterlab_sql_editor.ipython_magic.base import Base
from jupyterlab_sql_editor.ipython_magic.trino.trino_export import (
    update_database_schema,
)
from jupyterlab_sql_editor.ipython_magic.trino.trino_schema_widget import (
    TrinoSchemaWidget,
)
from jupyterlab_sql_editor.outputters.outputters import _display_results
from jupyterlab_sql_editor.outputters.util import _dedup_names, sanitize_results

VALID_OUTPUTS = ["sql", "text", "json", "html", "aggrid", "grid", "skip", "none"]


@magics_class
class Trino(Base):
    host = Unicode("localhost", config=True, help="Trino server hostname")
    port = Int(443, config=True, help="Trino server port number")
    auth = Instance(
        allow_none=True,
        klass="trino.auth.Authentication",
        config=True,
        help="An instance of the Trino Authentication class",
    )
    user = Unicode(
        "user",
        config=True,
        help="Trino user to use when no authentication is specified. This will set the HTTP header X-Trino-User",
    )
    catalog = Unicode("", config=True, help="Trino catalog to use")
    schema = Unicode("", config=True, help="Trino schema to use")
    httpScheme = Unicode("https", config=True, help="Trino server scheme https/http")
    verify = Union(
        [Bool(), Unicode()],
        default_value=True,
        config=True,
        help="Trino SSL verification. True or False to enable or disable SSL verification. /path/to/cert.crt for self-signed certificate.",
    )
    conn = None
    cur = None
    _conn_params = None

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)

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
        metavar="all|none",
        type=str,
        default="none",
        help="Force the regeneration of the schema cache file.",
    )
    @argument("-d", "--dataframe", metavar="name", type=str, help="Capture results in pandas dataframe")
    @argument("-i", "--input", metavar="name", type=str, help="Display pandas dataframe")
    @argument(
        "-o",
        "--output",
        metavar="sql|json|html|aggrid|grid|text|skip|none",
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
    @argument(
        "-x",
        "--raw",
        action="store_true",
        help="Run statement as is. Do not wrap statement with a limit. \
                    Use this option to run statement which can't be wrapped in a SELECT/LIMIT statement. \
                    For example EXPLAIN, SHOW TABLE, SHOW CATALOGS.",
    )
    @argument("--host", metavar="host", default=None, type=str)
    @argument("-c", "--catalog", metavar="catalogname", default=None, type=str, help="Trino catalog to use")
    @argument("-m", "--schema", metavar="schemaname", default=None, type=str, help="Trino schema to use")
    @argument("-j", "--jinja", action="store_true", help="Enable Jinja templating support")
    @argument("-t", "--truncate", metavar="max_cell_length", type=int, help="Truncate output")
    @argument("--expand", action="store_true", help="Expand json results")
    @argument(
        "-h",
        "--help",
        action="store_true",
        help="Detailed information about Trino magic",
    )
    @argument("--jsonnulls", action="store_true", help="Show nulls for JSON output")
    def trino(self, line=None, cell=None, local_ns=None):
        "Magic that works both as %trino and as %%trino"
        self.set_user_ns(local_ns)
        args = parse_argstring(self.trino, line)
        limit, truncate, output = self.parse_common_args(args, self.limit)
        catalog = args.catalog or self.catalog
        schema = args.schema or self.schema

        # Equivalent to %trino? or %%trino?
        if args.help:
            return self.show_help("trino")

        if args.transpile:
            return Pretty(
                sqlglot.transpile(
                    self.get_sql_statement(cell, args.sql, args.jinja), read="trino", write=args.transpile, pretty=True
                )[0]
            )

        if output not in VALID_OUTPUTS:
            print(f"Invalid output option {args.output}. The valid options are {VALID_OUTPUTS}.")
            return

        sql = self.set_query_limit(self.get_sql_statement(cell, args.sql, args.jinja), args.raw, limit)
        result_id = self.sql_hash(sql, limit) if sql else ""

        if args.input and cell is None:
            pdf = self.resolve_input_dataframe(args.input, cell, (pd.DataFrame,))
            _display_results(
                pdf=pdf,
                result_id=result_id,
                output=output,
                show_nonprinting=args.show_nonprinting,
                truncate=truncate,
                args=args,
            )
            return

        if self.check_refresh(catalog, schema, args.host if args.host else self.host, args.refresh.lower()):
            return

        if output == "sql":
            return self.display_sql(sql)

        # TODO: Rework caching feature
        # Use previously cached results if sql statement hasn't changed and is a SELECT type statement
        # use_cache = True if not args.nocache and sql == self.cached_sql and parsed[0].get_type() == "SELECT" else False
        use_cache = False
        if use_cache:
            print("Using cached results")

        results, columns = [], []
        if not (output == "skip" or output == "none") or args.dataframe:
            start = time()
            if not use_cache:
                self.conn = self.get_connection(catalog, schema, args.host if args.host else self.host)
                self.cur = self.conn.cursor()
                self.cur.execute(sql)
                results = self.cur.fetchmany(limit + 1)
                description = self.cur.description
                columns = list(map(lambda d: d[0], description)) if description else []
                schema = self.get_schema_from_query_description(description)
            else:
                pass
            end = time()

            self.print_execution_stats(end, start, len(results) if results else 0, limit)
            display(TrinoSchemaWidget("results", schema))
            results = results[:limit] if results else []
            pdf = self.get_pandas_dataframe(results, columns)

            if args.dataframe:
                print(f"Saved results to pandas dataframe named `{args.dataframe}`")
                self.get_shell().user_ns.update({args.dataframe: pdf})
        else:
            print("Display and execution of results skipped")
            return

        _display_results(
            pdf=pdf if pdf is not None else pd.DataFrame([]),
            output=output,
            result_id=result_id,
            show_nonprinting=args.show_nonprinting,
            truncate=truncate,
            args=args,
        )

    @staticmethod
    # https://github.com/apache/superset/blob/178607093fa826947d9130386705a2e3ed3d9a88/superset/sql_parse.py#L79-L97
    def _extract_limit_from_query(statement: TokenList) -> Optional[int]:
        """
        Extract limit clause from SQL statement.

        :param statement: SQL statement
        :return: Limit extracted from query, None if no limit present in statement
        """
        next_token = statement.token_next_by(m=(Keyword, "LIMIT"))
        if not next_token or next_token[0] is None:
            return None
        idx, _ = next_token
        result = statement.token_next(idx=idx)
        if not result or result[0] is None:
            return None
        _, token = result
        if token:
            if isinstance(token, IdentifierList):
                # In case of "LIMIT <offset>, <limit>", find comma and extract
                # first succeeding non-whitespace token
                comma_result = token.token_next_by(m=(sqlparse.tokens.Punctuation, ","))
                if comma_result and comma_result[0] is not None:
                    next_after_comma = token.token_next(idx=comma_result[0])
                    if next_after_comma and next_after_comma[0] is not None:
                        _, token = next_after_comma
            if token and token.ttype == sqlparse.tokens.Literal.Number.Integer:
                return int(token.value)
        return None

    def get_connection(self, catalog: str, schema: str, host: str):
        host = host or self.host
        catalog = catalog or self.catalog
        schema = schema or self.schema

        new_params = {
            "host": host,
            "port": self.port,
            "auth": self.auth,
            "user": self.user,
            "catalog": catalog,
            "schema": schema,
            "http_scheme": self.httpScheme,
            "verify": self.verify,
        }

        if self.conn is None or self._conn_params != new_params:
            self.conn = trino.dbapi.connect(**new_params)
            self._conn_params = new_params.copy()

        return self.conn

    def set_query_limit(self, sql: str, raw_query: bool, limit: int) -> str:
        sql_lim = 0
        sql_statement = sqlparse.format(sql, strip_comments=True)
        parsed = sqlparse.parse(sql_statement.strip(" \t\n;"))
        for statement in parsed:
            sql_lim = self._extract_limit_from_query(statement)
        if not raw_query and not sql_lim and parsed[0].get_type() == "SELECT":
            return f"{sql} \nLIMIT {limit + 1}"
        return sql

    def get_pandas_dataframe(self, results, columns) -> pd.DataFrame:
        pdf = pd.DataFrame.from_records(results, columns=columns)
        for c in pdf.columns:
            pdf[c] = pdf[c].apply(lambda v: sanitize_results(v))
        # dedup top-level column names
        pdf.columns = _dedup_names(pdf.columns.values.tolist())
        return pdf

    def get_schema_from_query_description(self, description) -> list:
        """
        Infer the schema of a query by inspecting the cursor's description.

        :param description: Cursor description.
        :return: A list of columns with metadata, or an empty list on failure.
        """
        try:
            if not description:
                raise ValueError("No description available; query may not return results.")

            # Build schema from description
            columns = []
            for col in description:
                columns.append(
                    {
                        "columnName": col.name,
                        "type": parse_sqltype(col.type_code),
                        "nullable": col.null_ok,
                        "default": None,
                    }
                )
            return columns
        except Exception as exc:
            logging.warning(f"Failed to get schema from query results: {exc}")
            return []

    def check_refresh(self, catalog, schema, host, refresh_arg) -> bool:
        output_file = (
            pathlib.Path(self.outputFile).expanduser()
            if self.outputFile
            else pathlib.Path("~/.local/trinodb.schema.json").expanduser()
        )
        catalog_array = self.get_catalog_array()

        if refresh_arg == "none":
            return False

        self.conn = self.get_connection(catalog, schema, host)
        if refresh_arg == "all":
            update_database_schema(self.conn, output_file, catalog_array)
        else:
            update_database_schema(self.conn, output_file, [refresh_arg])
        return True
