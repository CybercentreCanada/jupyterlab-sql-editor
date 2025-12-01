from pathlib import Path
from textwrap import dedent

from trino.dbapi import Connection
from trino.sqlalchemy.datatype import parse_sqltype

from jupyterlab_sql_editor.ipython_magic.export import (
    Catalog,
    ExportConnection,
    Function,
    SchemaExporter,
    Table,
    TrinoTableSchema,
)

MAX_RET = 20000


class TrinoExportConnection(ExportConnection):
    def __init__(self, cur) -> None:
        self.cur = cur

    def render_table(self, table: Table):
        columns = self._get_columns(table.catalog_name, table.database_name, table.table_name)
        return {
            "tableName": table.table_name,
            "columns": columns,
            "database": table.database_name,
            "catalog": table.catalog_name,
        }

    def render_function(self, function: Function):
        return {"name": function.function_name, "description": ""}

    def get_function_names(self) -> list[str]:
        sql = "SHOW FUNCTIONS"
        self.cur.execute(sql)
        rows = self.cur.fetchmany(MAX_RET)
        function_names = []
        for row in rows:
            name = row[0]
            if name not in function_names:
                function_names.append(name)
        return function_names

    def get_table_names(self, catalog_name: str, database_name: str) -> list[str]:
        # prevent retrieving tables from information_schema
        if database_name == "information_schema":
            return []
        path = f"{catalog_name}.{database_name}"
        try:
            sql = f"SHOW TABLES IN {path}"
            self.cur.execute(sql)
            rows = self.cur.fetchmany(MAX_RET)
            table_names = []
            for row in rows:
                table = row[0]
                table_names.append(table)
            return table_names
        except Exception:
            print(f"Failed to get tables for {path}")
            return []

    def get_database_names(self, catalog_name: str) -> list[str]:
        sql = f"SHOW SCHEMAS IN {catalog_name}"
        self.cur.execute(sql)
        rows = self.cur.fetchmany(MAX_RET)
        database_names = []
        for row in rows:
            database = row[0]
            database_names.append(database)
        return database_names

    def _get_columns(self, catalog_name: str, schema_name: str, table_name: str):
        try:
            query = dedent(
                f"""
                SELECT
                    *
                FROM {catalog_name}.{schema_name}.{table_name}
                WHERE 1 = 0
            """
            ).strip()
            self.cur.execute(query)
            description = self.cur.description or []
            columns = []
            for col in description:
                columns.append({"columnName": col.name, "type": parse_sqltype(col.type_code)})
            return TrinoTableSchema(columns, quoting_char='"').convert()
        except Exception as exc:
            print(f"Failed to get columns for {table_name}: {exc}")
            return []
        finally:
            if self.cur:
                self.cur.cancel()


def update_database_schema(conn: Connection, schema_file_name: Path, catalog_names: list[str]):
    cur = conn.cursor()
    connection = TrinoExportConnection(cur)
    catalogs: list[Catalog] = []
    for name in catalog_names:
        catalogs.append(Catalog(connection, name))
    exp = SchemaExporter(connection, schema_file_name, catalogs)
    exp.update_schema()
