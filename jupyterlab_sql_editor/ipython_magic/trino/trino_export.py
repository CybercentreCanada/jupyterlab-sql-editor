from pathlib import Path
from textwrap import dedent

from trino.sqlalchemy.datatype import parse_sqltype

from jupyterlab_sql_editor.ipython_magic.export import (
    Catalog,
    Connection,
    Function,
    SchemaExporter,
    Table,
    TrinoTableSchema,
)

MAX_RET = 20000


class TrinoConnection(Connection):
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

    def get_function_names(self):
        sql = "SHOW FUNCTIONS"
        self.cur.execute(sql)
        rows = self.cur.fetchmany(MAX_RET)
        function_names = []
        for row in rows:
            name = row[0]
            if name not in function_names:
                function_names.append(name)
        return function_names

    def get_table_names(self, catalog_name, database_name):
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

    def get_database_names(self, catalog_name):
        sql = f"SHOW SCHEMAS IN {catalog_name}"
        self.cur.execute(sql)
        rows = self.cur.fetchmany(MAX_RET)
        database_names = []
        for row in rows:
            database = row[0]
            database_names.append(database)
        return database_names

    def _get_columns(self, catalog_name, schema_name, table_name):
        try:
            query = dedent(
                f"""
                SELECT
                    column_name,
                    data_type,
                    IS_NULLABLE,
                    column_default
                FROM {catalog_name}.information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                ORDER BY ordinal_position ASC
            """
            ).strip()
            self.cur.execute(query)
            res = self.cur.fetchall()
            columns = []
            for record in res:
                column = dict(
                    columnName=record[0],
                    type=parse_sqltype(record[1]),
                    nullable=record[2] == "YES",
                    default=record[3],
                )
                columns.append(column)
            return TrinoTableSchema(columns, quoting_char='"').convert()
        except Exception as exc:
            print(f"Failed to get columns for {table_name}: {exc}")
            return []


def update_database_schema(cur, schema_file_name: Path, catalog_names):
    connection = TrinoConnection(cur)
    catalogs: list[Catalog] = []
    for name in catalog_names:
        catalogs.append(Catalog(connection, name))
    exp = SchemaExporter(connection, schema_file_name, catalogs, None)
    exp.update_schema()
