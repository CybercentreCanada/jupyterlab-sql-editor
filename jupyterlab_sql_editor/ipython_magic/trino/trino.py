import logging
import pathlib
from time import time
from typing import Optional

import pandas as pd
import sqlglot
import sqlparse
import trino
from IPython import get_ipython
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

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.cached_sql = None
        self.cached_results = None
        self.cached_pdf = None
        self.cached_schema = None

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

        # Equivalent to %trino? or %%trino?
        if args.help:
            ip = get_ipython()
            if ip:
                ip.run_line_magic("pinfo", "trino")
            return

        if args.transpile:
            return Pretty(
                sqlglot.transpile(
                    self.get_sql_statement(cell, args.sql, args.jinja), read="trino", write=args.transpile, pretty=True
                )[0]
            )

        output_file = (
            pathlib.Path(self.outputFile).expanduser()
            if self.outputFile
            else pathlib.Path("~/.local/trinodb.schema.json").expanduser()
        )

        truncate = 256
        if args.truncate:
            truncate = args.truncate

        catalog = args.catalog
        if not catalog:
            catalog = self.catalog

        schema = args.schema
        if not schema:
            schema = self.schema

        limit = args.limit
        if limit is None or limit <= 0:
            limit = self.limit

        if args.input and not isinstance(self.shell.user_ns.get(args.input), pd.DataFrame) and cell is None:
            print("Input does not exist or is not a pandas dataframe.")
            return

        if args.input and cell is not None:
            print("Ignoring --input, cell body found.")

        output = args.output.lower()
        if output not in VALID_OUTPUTS:
            print(f"Invalid output option {args.output}. The valid options are {VALID_OUTPUTS}.")
            return

        if args.input and cell is None:
            _display_results(
                pdf=self.shell.user_ns.get(args.input),
                output=output,
                show_nonprinting=args.show_nonprinting,
                truncate=truncate,
                args=args,
            )
            return

        self.conn = trino.dbapi.connect(
            host=args.host if args.host else self.host,
            port=self.port,
            auth=self.auth,
            user=self.user,
            source="ipython-magic",
            catalog=catalog,
            schema=schema,
            http_scheme=self.httpScheme,
            verify=self.verify,
        )
        self.cur = self.conn.cursor()

        catalog_array = self.get_catalog_array()
        if self.check_refresh(args.refresh.lower(), output_file, catalog_array):
            return

        sql = self.get_sql_statement(cell, args.sql, args.jinja)
        if not sql:
            return

        if output == "sql":
            return self.display_sql(sql)

        sql_lim = 0
        sql_statement = sqlparse.format(sql, strip_comments=True)
        parsed = sqlparse.parse(sql_statement.strip(" \t\n;"))
        for statement in parsed:
            sql_lim = self._extract_limit_from_query(statement)
        if not args.raw and not sql_lim and parsed[0].get_type() == "SELECT":
            sql = f"{sql} \nLIMIT {limit + 1}"

        # TODO: Rework caching feature
        # Use previously cached results if sql statement hasn't changed and is a SELECT type statement
        # use_cache = True if not args.nocache and sql == self.cached_sql and parsed[0].get_type() == "SELECT" else False
        use_cache = False
        if use_cache:
            print("Using cached results")

        results = [[]]
        columns = []
        if not (output == "skip" or output == "none") or args.dataframe:
            start = time()
            if not use_cache:
                self.cur.execute(sql)
                results = self.cur.fetchmany(limit + 1)
                description = self.cur.description
                columns = list(map(lambda d: d[0], description)) if description else []
                schema = self.get_schema_from_query_description(description)
            else:
                results = self.cached_results
                schema = self.cached_schema
            end = time()

            print(f"Execution time: {end - start:.2f} seconds")
            display(TrinoSchemaWidget("results", schema))
        else:
            print("Display and execution of results skipped")
            return

        self.cached_results = results
        results_length = len(results) if results else 0
        if results_length > limit and not (output == "skip" or output == "none"):
            print(f"Only showing top {limit} {'row' if limit == 1 else 'rows'}")
            results = results[:limit] if results else [[]]

        if not use_cache:
            pdf = pd.DataFrame.from_records(results, columns=columns)
            for c in pdf.columns:
                pdf[c] = pdf[c].apply(lambda v: sanitize_results(v))
            # dedup top-level column names
            pdf.columns = _dedup_names(pdf.columns.values.tolist())
        else:
            pdf = self.cached_pdf

        if args.dataframe:
            print(f"Saved results to pandas dataframe named `{args.dataframe}`")
            self.shell.user_ns.update({args.dataframe: pdf})

        # Cache results
        self.cached_sql = sql
        self.cached_pdf = pdf.copy() if pdf is not None else None
        self.cached_schema = schema

        _display_results(
            pdf=pdf if pdf is not None else pd.DataFrame([]),
            output=output,
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
        idx, _ = statement.token_next_by(m=(Keyword, "LIMIT"))
        if idx is not None:
            _, token = statement.token_next(idx=idx)
            if token:
                if isinstance(token, IdentifierList):
                    # In case of "LIMIT <offset>, <limit>", find comma and extract
                    # first succeeding non-whitespace token
                    idx, _ = token.token_next_by(m=(sqlparse.tokens.Punctuation, ","))
                    _, token = token.token_next(idx=idx)
                if token and token.ttype == sqlparse.tokens.Literal.Number.Integer:
                    return int(token.value)
        return None

    def get_schema_from_query_description(self, description):
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

    def check_refresh(self, refresh_arg, output_file, catalog_array):
        if refresh_arg == "none":
            return False
        if refresh_arg == "all":
            update_database_schema(self.cur, output_file, catalog_array)
        else:
            update_database_schema(self.cur, output_file, [refresh_arg])
        return True
