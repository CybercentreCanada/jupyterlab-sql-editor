import logging

from pyspark.sql.types import StringType, StructType
from trino.exceptions import TrinoUserError

from jupyterlab_sql_editor.ipython_magic.export import (
    Catalog,
    Connection,
    Function,
    SchemaExporter,
    SparkTableSchema,
    Table,
)
from jupyterlab_sql_editor.ipython_magic.trino.parser import trino_column_parser

MAX_RET = 20000


class TrinoConnection(Connection):
    def __init__(self, cur) -> None:
        self.cur = cur

    def render_table(self, table: Table):
        full_table_name = table.catalog_name + "." + table.database_name + "." + table.table_name
        columns = self._get_columns(full_table_name)
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
        # print(sql)
        self.cur.execute(sql)
        rows = self.cur.fetchmany(MAX_RET)
        # initialize a null list
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
            # print(sql)
            self.cur.execute(sql)
            rows = self.cur.fetchmany(MAX_RET)
            table_names = []
            for row in rows:
                table = row[0]
                table_names.append(table)
            return table_names
        except TrinoUserError:
            print(f"Failed to get tables for {path}")
            return []

    def get_database_names(self, catalog_name):
        sql = f"SHOW SCHEMAS IN {catalog_name}"
        # print(sql)
        self.cur.execute(sql)
        rows = self.cur.fetchmany(MAX_RET)
        database_names = []
        for row in rows:
            database = row[0]
            database_names.append(database)
        return database_names

    def _get_columns(self, table_name):
        try:
            sql = f"SHOW COLUMNS IN {table_name}"
            # print(sql)
            self.cur.execute(sql)
            rows = self.cur.fetchmany(MAX_RET)
            schema = StructType()
            for row in rows:
                name = row[0]
                row_schema = row[1]
                try:
                    column_type = trino_column_parser.parse(row_schema)
                    schema.add(name, column_type)
                except Exception:
                    logging.warn(f"failed to parse column with schema {row_schema}")
                    schema.add(name, StringType())

            return SparkTableSchema(schema, quoting_char='"').convert()
        except TrinoUserError:
            print(f"Failed to get columns for {table_name}")
            return []


def update_database_schema(cur, schema_file_name, catalog_names):
    connection = TrinoConnection(cur)
    catalogs: list(Catalog) = []
    for name in catalog_names:
        catalogs.append(Catalog(connection, name))
    exp = SchemaExporter(connection, schema_file_name, catalogs, None)
    exp.update_schema()
