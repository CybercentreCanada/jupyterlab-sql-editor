import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from jupyterlab_sql_editor.ipython_magic.common.export import (
    Function,
    SchemaExporter,
    Connection,
    Catalog,
    Table,
)


class SparkTableSchema:
    def __init__(self, schema) -> None:
        self.schema = schema

    _FIELD_TYPES = {
        StringType: "string",
        ArrayType: "array",
        TimestampType: "timestamp",
        DateType: "date",
        LongType: "long",
        IntegerType: "integer",
        BooleanType: "integer",
        StructType: "struct",
        MapType: "map",
        DecimalType: "decimal",
        DoubleType: "double",
        FloatType: "float",
        ShortType: "short",
        BinaryType: "binary",
    }

    def get_type_name(self, field_type):
        return self._FIELD_TYPES.get(type(field_type))

    def get_path(self, path, name):
        if " " in name:
            name = "`" + name + "`"
        if len(path) > 0:
            return f"{path}.{name}"
        return name

    def get_children(self, field, path, fields):
        if isinstance(field, StructField):
            self.get_children(field.dataType, self.get_path(path, field.name), fields)
        elif isinstance(field, MapType):
            self.get_children(field.valueType, self.get_path(path, "key"), fields)
        elif isinstance(field, ArrayType):
            self.get_children(field.elementType, path, fields)
        elif isinstance(field, StructType):
            for name in field.fieldNames():
                child = field[name]
                fields.append(
                    {
                        "columnName": self.get_path(path, name),
                        "metadata": child.metadata,
                        "type": self.get_type_name(child.dataType),
                        "description": self.get_type_name(child.dataType),
                    }
                )
                self.get_children(child, path, fields)

    def convert(self):
        fields = []
        self.get_children(self.schema, "", fields)
        return fields


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
        self.spark.sql(f"USE {catalog_name}")
        if database_name:
            rows = self.spark.sql(f"SHOW TABLES IN {database_name}").collect()
        else:
            rows = self.spark.sql(f"SHOW TABLES").collect()
        for r in rows:
            # depending if iceberg catalogs are use you might get results
            # with either a database or namespace column
            if (
                getattr(r, "database", "") == database_name
                or getattr(r, "namespace", "") == database_name
            ):
                table_names.append(r["tableName"])
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


# spark = SparkSession.builder.appName("abc").getOrCreate()

# spark.sql("select 'allo'").createOrReplaceTempView("view_no_database")

# spark.sql("create database db1").collect()
# spark.sql("create database db2").collect()

# spark.sql(
#     """
# CREATE OR REPLACE VIEW view_default_database
#     (ID COMMENT 'Unique identification number', Name)
#     COMMENT 'View for experienced employees'
#     AS SELECT 1 as id, 'jc' as name
# """
# ).collect()

# spark.sql(
#     """
# CREATE OR REPLACE VIEW db1.view_in_db1
#     (ID COMMENT 'Unique identification number', Name)
#     COMMENT 'View for experienced employees'
#     AS SELECT 1 as id, 'jc' as name
# """
# ).collect()

# spark.sql("use spark_catalog").show()
# spark.sql("use spark_catalog.db1").show()

# spark.sql(
#     """
# show tables
# """
# ).show()

# spark.table("view_no_database").printSchema()
# spark.table("default.view_default_database").printSchema()
# spark.table("db1.view_in_db1").printSchema()


def update_database_schema(spark, schema_file_name, catalog_names):
    connection = SparkConnection(spark)
    local_catalog = Catalog(connection, "spark_catalog")
    catalogs: list(Catalog) = []
    for name in catalog_names:
        catalogs.append(Catalog(connection, name))
    catalogs.append(local_catalog)
    exp = SchemaExporter(connection, schema_file_name, catalogs, local_catalog)
    exp.update_schema()


def update_local_database(spark, schema_file_name):
    connection = SparkConnection(spark)
    local_catalog = Catalog(connection, "spark_catalog")
    exp = SchemaExporter(connection, schema_file_name, None, local_catalog)
    exp.update_local_schema()
