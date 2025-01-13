from __future__ import annotations

import json
import os
import pathlib
import time
from abc import ABC, abstractmethod
from typing import Any

from IPython.display import clear_output
from pyspark.sql.types import (
    ArrayType,
    AtomicType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    FractionalType,
    IntegerType,
    IntegralType,
    LongType,
    MapType,
    NullType,
    NumericType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
)
from sqlalchemy.sql import sqltypes
from trino.sqlalchemy.datatype import DOUBLE, JSON, MAP, ROW, TIME, TIMESTAMP

from .util import merge_schemas


class Connection(ABC):
    @abstractmethod
    def render_table(self, table: Table) -> dict[str, Any]:
        pass

    @abstractmethod
    def render_function(self, function: Function) -> dict[str, Any]:
        pass

    @abstractmethod
    def get_function_names(self) -> list:
        pass

    @abstractmethod
    def get_table_names(self, catalog_name, database_name) -> list:
        pass

    @abstractmethod
    def get_database_names(self, catalog_name) -> list:
        pass


class Catalog:
    def __init__(self, connection: Connection, catalog_name) -> None:
        self.connection = connection
        self.catalog_name = catalog_name
        self.databases: list[Database] = []

    def populate_databases(self):
        print(f"Listing tables in {self.catalog_name}")
        database_names = self.connection.get_database_names(self.catalog_name)
        for database_name in database_names:
            self.databases.append(Database(self.connection, self.catalog_name, database_name))

    def get_tables(self):
        self.populate_databases()
        catalog_tables: list[Table] = []
        for d in self.databases:
            catalog_tables = catalog_tables + d.get_tables()
        return catalog_tables


class Database:
    def __init__(self, connection: Connection, catalog_name: str, database_name: str) -> None:
        self.connection = connection
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.tables: list[Table] = []

    def populate_tables(self):
        table_names = self.connection.get_table_names(self.catalog_name, self.database_name)
        for table_name in table_names:
            try:
                self.tables.append(Table(self.connection, self.catalog_name, self.database_name, table_name))
            except Exception:
                # Skip problematic tables
                pass

    def get_tables(self):
        self.populate_tables()
        return self.tables


class Table:
    def __init__(self, connection: Connection, catalog_name, database_name, table_name) -> None:
        self.connection = connection
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.table_name = table_name

    def render(self):
        return self.connection.render_table(self)


class FunctionList:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.functions: list[Function] = []

    def populate_functions(self):
        function_names = self.connection.get_function_names()
        for function_name in function_names:
            self.functions.append(Function(self.connection, function_name))

    def get_functions(self):
        self.populate_functions()
        return self.functions


class Function:
    def __init__(self, connection: Connection, function_name) -> None:
        self.connection = connection
        self.function_name = function_name

    def render(self):
        return self.connection.render_function(self)


class SchemaExporter:
    def __init__(
        self,
        connection: Connection,
        schema_file_name,
        catalogs: list[Catalog],
        local_catalog: Catalog,
        display_progress: bool = True,
    ) -> None:
        self.connection = connection
        self.schema_file_name = schema_file_name
        self.function_list = FunctionList(connection)
        self.catalogs = catalogs
        self.local_catalog = local_catalog
        self.display_progress = display_progress

    def update_progress(self, message, progress):
        if self.display_progress:
            bar_length = 40
            if isinstance(progress, int):
                progress = float(progress)
            if not isinstance(progress, float):
                progress = 0
            if progress < 0:
                progress = 0
            if progress >= 1:
                progress = 1

            block = int(round(bar_length * progress))

            clear_output(wait=True)
            text = "{0}: [{1}] {2:.1f}%".format(message, "#" * block + "-" * (bar_length - block), progress * 100)
            print(text)

    def render_functions(self):
        functions = self.function_list.get_functions()
        num_functions = len(functions)
        rendered_functions = []
        for idx, f in enumerate(functions):
            rendered_functions.append(f.render())
            self.update_progress("Exporting functions", idx / num_functions)
        self.update_progress("Exporting functions", 1)
        return rendered_functions

    def render_catalog(self, catalog: Catalog):
        rendered_tables = []
        tables = catalog.get_tables()
        num_tables = len(tables)
        for idx, t in enumerate(tables):
            try:
                rendered_tables.append(t.render())
                self.update_progress(f"Exporting tables from {catalog.catalog_name}", idx / num_tables)
            except Exception:
                # Skip problematic tables
                pass
        self.update_progress(f"Exporting tables from {catalog.catalog_name}", 1)
        return rendered_tables

    def render_catalogs(self):
        rendered_tables = []
        for catalog in self.catalogs:
            rendered_tables = rendered_tables + self.render_catalog(catalog)
        return rendered_tables

    def should_update_schema(self, refresh_threshold):
        file_exists = os.path.isfile(self.schema_file_name)
        ttl_expired = False
        if file_exists:
            file_time = os.path.getmtime(self.schema_file_name)
            current_time = time.time()
            if current_time - file_time > refresh_threshold:
                print(f"TTL {refresh_threshold} minutes expired, re-generating schema file: {self.schema_file_name}")
                ttl_expired = True

        return (not file_exists) or ttl_expired

    def update_schema(self):
        print(f"Generating schema file: {self.schema_file_name}")

        # Create folders if they don't exist
        pathlib.Path(self.schema_file_name).parent.mkdir(parents=True, exist_ok=True)

        # Load the existing schema if the file exists
        existing_schema = {}
        if pathlib.Path(self.schema_file_name).exists():
            with open(self.schema_file_name, "r", encoding="utf8") as fin:
                try:
                    existing_schema = json.load(fin)
                except json.JSONDecodeError:
                    print(f"Could not decode JSON from {self.schema_file_name}. Proceeding with an empty schema.")
                    existing_schema = {}

        new_schema = {
            "tables": self.render_catalogs(),
            "functions": self.render_functions(),
        }

        # Merge new_schema into existing_schema
        merge_schemas(existing_schema, new_schema)

        # Save schema to disk. sql-language-server will pickup any changes to this file.
        with open(self.schema_file_name, "w", encoding="utf8") as fout:
            json.dump(existing_schema, fout, sort_keys=True, indent=2)

        print(f"Schema file updated: {self.schema_file_name}")

    def update_local_schema(self):
        print("Updating local tables")
        updated_tables = self.render_catalog(self.local_catalog)
        current_schema = {}
        with open(self.schema_file_name, "r", encoding="utf8") as file:
            current_schema = json.load(file)

        for table in current_schema["tables"]:
            if table["catalog"]:
                updated_tables.append(table)

        updated_schema = {
            "tables": updated_tables,
            "functions": current_schema["functions"],
        }

        with open(self.schema_file_name, "w", encoding="utf8") as fout:
            json.dump(updated_schema, fout, indent=2, sort_keys=True)

        print(f"Schema file updated: {self.schema_file_name}")


class SparkTableSchema:
    # TODO: consider using an alternative abstraction rather than using
    # spark's model.
    def __init__(self, schema, quoting_char="`") -> None:
        self.schema = schema
        self.quoting_char = quoting_char

    _FIELD_TYPES = {
        AtomicType: "atomic",
        ByteType: "byte",
        DataType: "data",
        DayTimeIntervalType: "daytime_interval",
        FractionalType: "fractional",
        IntegralType: "integral",
        NullType: "null",
        NumericType: "numeric",
        TimestampNTZType: "timestamp_ntz",
        UserDefinedType: "udf",
        StringType: "string",
        ArrayType: "array",
        TimestampType: "timestamp",
        DateType: "date",
        LongType: "long",
        IntegerType: "integer",
        BooleanType: "boolean",
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
        if " " in name or name[0].isdigit():
            name = self.quoting_char + name + self.quoting_char
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


class TrinoTableSchema:
    def __init__(self, schema, quoting_char="`") -> None:
        self.schema = schema
        self.quoting_char = quoting_char

    _FIELD_TYPES = {
        # === Boolean ===
        sqltypes.BOOLEAN: "boolean",
        # === Integer ===
        sqltypes.SMALLINT: "smallint",
        sqltypes.INTEGER: "integer",
        sqltypes.BIGINT: "bigint",
        # === Floating-point ===
        sqltypes.REAL: "real",
        DOUBLE: "double",
        # === Fixed-precision ===
        sqltypes.DECIMAL: "decimal",
        # === String ===
        sqltypes.VARCHAR: "varchar",
        sqltypes.CHAR: "char",
        sqltypes.VARBINARY: "varbinary",
        JSON: "json",
        # === Date and time ===
        sqltypes.DATE: "date",
        TIME: "time",
        TIMESTAMP: "timestamp",
        # === Structural ===
        sqltypes.ARRAY: "array",
        MAP: "map",
        ROW: "row",
        # === Others ===
        # IPADDRESS: 'ipaddress',
        # UUID: 'uuid',
        # HYPERLOGLOG: 'hyperloglog',
        # P4HYPERLOGLOG: 'p4hyperloglog',
        # SETDIGEST: 'setdigest',
        # QDIGEST: 'qdigest',
        # TDIGEST: 'tdigest',
    }

    def get_type_name(self, field_type):
        result = self._FIELD_TYPES.get(type(field_type))
        return result

    def get_path(self, path, name):
        if " " in name or name[0].isdigit():
            name = self.quoting_char + name + self.quoting_char
        if len(path) > 0:
            return f"{path}.{name}"
        return name

    def get_children(self, field, path, fields):
        if isinstance(field, dict):
            self.get_children(field["type"], self.get_path(path, field["columnName"]), fields)
        elif isinstance(field, MAP):
            self.get_children(field.value_type, self.get_path(path, "key"), fields)
        elif isinstance(field, sqltypes.ARRAY):
            self.get_children(field.item_type, path, fields)
        elif isinstance(field, ROW):
            for attr_name, attr_type in field.attr_types:
                type_name = self.get_type_name(attr_type)
                fields.append(
                    {
                        "columnName": self.get_path(path, attr_name),
                        "description": type_name,
                        "type": type_name,
                        "metadata": {},
                    }
                )
                self.get_children(attr_type, self.get_path(path, attr_name), fields)
        elif isinstance(field, (list)):
            for f in field:
                type_name = self.get_type_name(f.get("type"))
                fields.append(
                    {
                        "columnName": self.get_path(path, f.get("columnName")),
                        "description": type_name,
                        "type": type_name,
                        "metadata": {},
                    }
                )
                self.get_children(f, path, fields)

    def convert(self):
        fields = []
        self.get_children(self.schema, "", fields)
        return fields
