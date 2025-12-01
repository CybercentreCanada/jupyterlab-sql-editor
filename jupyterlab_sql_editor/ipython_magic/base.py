import hashlib
import os
import pathlib
import time
from abc import abstractmethod
from typing import Any

from IPython.core.getipython import get_ipython
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import Magics, magics_class
from IPython.display import Code
from jinja2 import StrictUndefined, Template
from traitlets import Bool, Int, Unicode

from jupyterlab_sql_editor.ipython_magic.messages import HOW_TO_ESCAPE_MSG, VARIABLE_NOT_FOUND_MSG

DEFAULT_SCHEMA_TTL = -1
DEFAULT_CATALOGS = ""


class ExplainUndefined(StrictUndefined):
    __slots__ = ()

    def __str__(self):
        print(VARIABLE_NOT_FOUND_MSG.format(var_name=self._undefined_name))
        print(HOW_TO_ESCAPE_MSG)
        super().__str__()


@magics_class
class Base(Magics):
    limit = Int(20, config=True, help="The maximum number of rows to display")
    cacheTTL = Int(
        DEFAULT_SCHEMA_TTL,
        config=True,
        help=f"Re-generate output schema file if older than time specified (defaults to {DEFAULT_SCHEMA_TTL} minutes)",
    )
    catalogs = Unicode(
        DEFAULT_CATALOGS,
        config=True,
        help=f'Retrive schema from the specified list of catalogs (defaults to "{DEFAULT_CATALOGS}")',
    )
    interactive = Bool(False, config=True, help="Display results in interactive grid")
    outputFile = Unicode("", config=True, help="Output schema to specified file")

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)
        self.user_ns = {}

    @property
    @abstractmethod
    def default_schema_file(self) -> str:
        """Return the default schema file name."""
        pass

    @abstractmethod
    def session(
        self, catalog: str | None = None, schema: str | None = None, host: str | None = None, source: str | None = None
    ) -> Any:
        """Return the connection/session object (Trino Connection or SparkSession)."""
        pass

    @abstractmethod
    def update_schema(self, target, output_file, catalog_array):
        """Perform the schema update using the provided target/session."""
        pass

    @abstractmethod
    def update_local_schema(self, target, output_file, catalog_array):
        """Optional: for Spark-specific local update."""
        pass

    @abstractmethod
    def resolve_sql(self, cell: str | None, sql_args: list[str], dbt: bool, jinja: bool) -> str:
        """Return the resolved SQL statement, raise ValueError if none."""
        pass

    def check_refresh(self, refresh_arg, catalog=None, schema=None, host=None, cache_ttl=-1) -> bool:
        """Returns True if a refresh was performed."""
        output_file = pathlib.Path(
            getattr(self, "outputFile", None) or f"~/.local/{self.default_schema_file}"
        ).expanduser()

        if refresh_arg == "none":
            return False

        if self.should_update_schema(output_file, cache_ttl):
            catalog_array = self.get_catalog_array()

            target = self.session(catalog=catalog, schema=schema, host=host)

            if refresh_arg == "all":
                self.update_schema(target, output_file, catalog_array)
            elif refresh_arg == "local":
                self.update_local_schema(target, output_file, catalog_array)
            else:
                self.update_schema(target, output_file, [refresh_arg])

            return True
        return False

    def get_shell(self) -> InteractiveShell:
        if self.shell:
            return self.shell
        raise AttributeError("Shell not found")

    def show_help(self, magic_name: str) -> None:
        ip = get_ipython()
        if ip:
            ip.run_line_magic("pinfo", magic_name)

    def resolve_input_dataframe(self, input_name: str, cell: str | None, valid_types: tuple) -> Any:
        if input_name and cell is None:
            obj = self.get_shell().user_ns.get(input_name)
            if not isinstance(obj, valid_types):
                actual_type = type(obj).__name__ if obj is not None else "None"
                raise TypeError(f"Expected one of {valid_types} for variable '{input_name}', but got {actual_type}.")
            return obj
        if input_name and cell is not None:
            print("Ignoring --input, cell body found.")
        return None

    def parse_common_args(self, args, default_limit: int, default_truncate: int = 256) -> tuple[int, int, str]:
        """Return (limit, truncate, output) with defaults applied."""
        truncate = args.truncate or default_truncate
        limit = args.limit if args.limit and args.limit > 0 else default_limit
        output = args.output.lower()
        return limit, truncate, output

    def get_catalog_array(self) -> list[str]:
        catalog_array = []
        if "," in self.catalogs:
            catalog_array = self.catalogs.split(",")
        return catalog_array

    def get_sql_statement(self, cell, sql_argument, use_jinja) -> str:
        sql = cell
        if cell is None:
            sql = " ".join(sql_argument)
        if sql and use_jinja:
            return self.bind_variables(sql, self.user_ns)
        return sql

    def set_user_ns(self, local_ns) -> None:
        if local_ns is None:
            local_ns = {}

        self.user_ns = self.get_shell().user_ns.copy()
        self.user_ns.update(local_ns)

    @staticmethod
    def print_execution_stats(end_time, start_time, results_length, limit) -> None:
        print(f"Execution time: {end_time - start_time:.2f} seconds")
        if results_length > limit:
            print(f"Only showing top {limit} {'row' if limit == 1 else 'rows'}")

    @staticmethod
    def display_sql(sql):
        return Code(data=sql, language="mysql")

    @staticmethod
    def sql_hash(sql: str, limit: int) -> str:
        key = f"{sql.strip()}::{limit}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    @staticmethod
    def bind_variables(query, user_ns):
        template = Template(query, undefined=ExplainUndefined)
        return template.render(user_ns)

    @staticmethod
    def should_update_schema(schema_file_name, refresh_threshold_seconds) -> bool:
        if not os.path.isfile(schema_file_name):
            print(f"Schema file does not exist, will generate {schema_file_name}")
            return True

        file_time = os.path.getmtime(schema_file_name)
        age_seconds = time.time() - file_time

        if age_seconds > refresh_threshold_seconds:
            print(
                f"Schema cache TTL of {refresh_threshold_seconds} seconds expired "
                f"(file age: {age_seconds:.0f}s), re-generating schema file {schema_file_name}"
            )
            return True

        print(
            f"Schema file within TTL ({age_seconds:.0f}s < {refresh_threshold_seconds}s), "
            f"skipping regeneration of {schema_file_name}"
        )

        return False
