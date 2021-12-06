import json
import os
import time

from pyspark.sql.types import * # pylint: disable=unused-wildcard-import, wildcard-import


_FIELD_TYPES = {
    StringType: 'string',
    ArrayType: 'array',
    TimestampType: 'timestamp',
    DateType: 'date',
    LongType: 'long',
    IntegerType: 'integer',
    BooleanType: 'integer',
    StructType: 'struct',
    MapType: 'map',
    DecimalType: 'decimal',
    DoubleType: 'double',
    FloatType: 'float',
    ShortType: 'short',
    BinaryType: 'binary'
}


def get_type_name(field_type):
    return _FIELD_TYPES.get(type(field_type))


def get_path(path, name):
    if ' ' in name:
        name = '`' + name + '`'
    if len(path) > 0:
        return f'{path}.{name}'
    return name


def get_children(field, path, fields):
    if isinstance(field, StructField):
        get_children(field.dataType, get_path(path, field.name), fields)
    elif isinstance(field, MapType):
        get_children(field.valueType, get_path(path, 'key'), fields)
    elif isinstance(field, ArrayType):
        get_children(field.elementType, path, fields)
    elif isinstance(field, StructType):
        for name in field.fieldNames():
            child = field[name]
            fields.append({
                'columnName': get_path(path, name),
                'metadata': child.metadata,
                'type': get_type_name(child.dataType),
                'description': get_type_name(child.dataType)
            })
            get_children(child, path, fields)


def get_columns(spark, name):
    fields = []
    get_children(spark.table(name).schema, '', fields)
    return fields


def get_tables_in_database(spark, catalog, database):
    spark.sql(f'USE {catalog}')
    rows = spark.sql(f'SHOW TABLES IN {database}').collect()
    if catalog == 'default':
        return list(map(lambda r: {
            "tableName": r.tableName,
            "columns": get_columns(spark, database + '.' + r.tableName),
            "database": database,
            "catalog": None
        }, rows))
    return list(map(lambda r: {
        "tableName": r.tableName,
        "columns": get_columns(spark, catalog + '.' + database + '.' + r.tableName),
        "database": database,
        "catalog": catalog
    }, rows))


def get_tables_in_catalogs(spark, catalogs):
    tables = []
    for catalog in catalogs:
        print('Listing tables in {catalog}')
        spark.sql(f'USE {catalog}')
        rows = spark.sql('SHOW DATABASES').collect()
        for row in rows:
            tables = tables + get_tables_in_database(spark, catalog, row.namespace)
    return tables


def get_description(spark, name):
    rows = spark.sql(f'DESCRIBE FUNCTION EXTENDED {name}').collect()
    text_lines = list(map(lambda r: r.function_desc, rows))
    return "\n".join(text_lines)


def get_functions(spark):
    spark.sql('USE spark_catalog')
    rows = spark.sql('SHOW FUNCTIONS').collect()
    return list(map(lambda f: {
        "name": f.function,
        "description": get_description(spark, f.function)
    }, rows))


def get_spark_database_schema(spark, catalogs):
    return {
        "functions": get_functions(spark),
        "tables": get_tables_in_catalogs(spark, catalogs) + get_tables_in_local_database(spark)
    }


def get_tables_in_local_database(spark):
    spark.sql('USE spark_catalog')
    rows = spark.sql('SHOW TABLES IN default').collect()
    return list(map(lambda r: {
        "tableName": r.tableName,
        "columns": get_columns(spark, r.tableName),
        "database": None,
        "catalog": None
    }, rows))


def should_update_schema(schema_file_name, refresh_threshold):
    file_exists = os.path.isfile(schema_file_name)
    ttl_expired = False
    if file_exists:
        file_time = os.path.getmtime(schema_file_name)
        current_time = time.time()
        if current_time - file_time > refresh_threshold:
            print(f'TTL {refresh_threshold} seconds expired, re-generating schema file: {schema_file_name}')
            ttl_expired = True

    return (not file_exists) or ttl_expired


def update_database_schema(spark, schema_file_name, catalogs):
    print(f'Generating schema file: {schema_file_name}')
    sparkdb_schema = get_spark_database_schema(spark, catalogs)
    # Save schema to disk. sql-language-server will pickup any changes to this file.
    with open(schema_file_name, 'w', encoding="utf8") as fout:
        json.dump(sparkdb_schema, fout, sort_keys=True, indent=2)
    print('Schema file updated: ' + schema_file_name)


def update_local_database(spark, schema_file_name):
    print('Updating local tables')
    updated_tables = get_tables_in_local_database(spark)
    current_schema = {}
    with open(schema_file_name, 'r', encoding="utf8") as file:
        current_schema = json.load(file)

    for table in current_schema['tables']:
        if table['catalog']:
            updated_tables.append(table)

    updated_schema = {
        "tables": updated_tables,
        "functions": current_schema['functions']
    }

    with open(schema_file_name, 'w', encoding="utf8") as fout:
        json.dump(updated_schema, fout, indent=2, sort_keys=True)
