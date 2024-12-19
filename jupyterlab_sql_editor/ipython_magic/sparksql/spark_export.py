from pathlib import Path

from jupyterlab_sql_editor.ipython_magic.export import (
    Catalog,
    Connection,
    Function,
    SchemaExporter,
    SparkTableSchema,
    Table,
)


class SparkConnection(Connection):
    def __init__(self, spark) -> None:
        self.spark = spark

    def render_table(self, table: Table):
        catalog_name = table.catalog_name
        database_name = table.database_name
        table_name = table.table_name

        if catalog_name == "spark_catalog":
            catalog_name = None

        if database_name == "":
            database_name = None

        full_table_name = table_name

        if database_name:
            full_table_name = database_name + "." + full_table_name
        if catalog_name:
            full_table_name = catalog_name + "." + full_table_name

        catalog = catalog_name if catalog_name else "spark_catalog"
        self.spark.sql(f"USE {catalog}")
        columns = self._get_columns(full_table_name)
        return {
            "columns": columns,
            "tableName": table_name,
            "database": database_name,
            "catalog": catalog_name,
        }

    def render_function(self, function: Function):
        return {
            "name": function.function_name,
            "description": self._get_description(function.function_name),
        }

    def _get_columns(self, full_table_name):
        schema = self.spark.table(full_table_name).schema
        table_schema = SparkTableSchema(schema)
        return table_schema.convert()

    def get_function_names(self):
        self.spark.sql("USE spark_catalog")
        rows = self.spark.sql("SHOW FUNCTIONS").collect()
        function_names = []
        for r in rows:
            function_names.append(r.function)
        return function_names

    def _get_description(self, function_name):
        rows = self.spark.sql(f"DESCRIBE FUNCTION EXTENDED {function_name}").collect()
        text_lines = list(map(lambda r: r.function_desc, rows))
        return "\n".join(text_lines)

    def get_table_names(self, catalog_name, database_name):
        table_names = []
        try:
            if catalog_name and database_name:
                rows = self.spark.sql(f"SHOW TABLES IN {catalog_name}.{database_name}").collect()
            else:
                rows = self.spark.sql("SHOW TABLES").collect()
            for r in rows:
                # depending if iceberg catalogs are use you might get results
                # with either a database or namespace column
                if getattr(r, "database", "") == database_name or getattr(r, "namespace", "") == database_name:
                    table_names.append(r["tableName"])
        except Exception:
            # Skip problematic database
            (
                print(f"Exception listing tables for {catalog_name}.{database_name}")
                if catalog_name and database_name
                else print("Exception listing tables")
            )
        return table_names

    def get_database_names(self, catalog_name):
        self.spark.sql(f"USE {catalog_name}")
        rows = self.spark.sql("SHOW DATABASES").collect()
        database_names = []
        for r in rows:
            database_names.append(r["namespace"])
        if catalog_name == "spark_catalog":
            database_names.append("")
        return database_names


def update_database_schema(spark, schema_file_name, catalog_names):
    connection = SparkConnection(spark)
    local_catalog = Catalog(connection, "spark_catalog")
    catalogs: list[Catalog] = []
    for name in catalog_names:
        catalogs.append(Catalog(connection, name))
    catalogs.append(local_catalog)
    exp = SchemaExporter(connection, schema_file_name, catalogs, local_catalog)
    exp.update_schema()


def update_local_database(spark, schema_file_name: Path, catalog_array):
    # If file doesn't exist, just do a --refresh all instead
    if not schema_file_name.exists():
        update_database_schema(spark, schema_file_name, catalog_array)
        return
    connection = SparkConnection(spark)
    local_catalog = Catalog(connection, "spark_catalog")
    exp = SchemaExporter(connection, schema_file_name, None, local_catalog, display_progress=False)
    exp.update_local_schema()
