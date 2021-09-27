import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os.path as path
import time


def getTypeName(t):
    if type(t) == LongType:
        return 'long'
    if type(t) == IntegerType:
        return 'integer'
    if type(t) == StringType:
        return 'string'
    if type(t) == ArrayType:
        return 'array'
    if type(t) == StructType:
        return 'struct'
    if type(t) == MapType:
        return 'map'


def getPath(path, name):
    if ' ' in name:
        name = '`' + name + '`'
    if len(path) > 0:
        return f'{path}.{name}'
    return name


def getChildren(field, path, fields):
    if type(field) == StructField:
        getChildren(field.dataType, getPath(path, field.name), fields)
    elif type(field) == MapType:
        getChildren(field.valueType, getPath(path, 'key'), fields)
    elif type(field) == ArrayType:
        getChildren(field.elementType, path, fields)
    elif type(field) == StructType:
        for name in field.fieldNames():
            child = field[name]
            fields.append({
                'columnName': getPath(path, name),
                'metadata': child.metadata,
                'type': getTypeName(child.dataType),
                'description': getTypeName(child.dataType)
            })
            getChildren(child, path, fields)


def getColumns(spark, name):
    fields = []
    getChildren(spark.table(name).schema, '', fields)
    return fields


def getTablesInDatabase(spark, catalog, database):
    spark.sql(f'USE {catalog}')
    rows = spark.sql(f'SHOW TABLES IN {database}').collect()
    return list(map(lambda r: {
        "tableName": r.tableName,
        "columns": getColumns(spark, catalog + '.' + database + '.' + r.tableName),
        "database": database,
        "catalog": catalog
    }, rows))

def getTablesInCatalogs(spark, catalogs):
    tables = []
    for catalog in catalogs:
        print(f'getting tables in {catalog}')
        spark.sql(f'USE {catalog}')
        rows = spark.sql(f'SHOW DATABASES').collect()
        for row in rows:
            tables = tables + getTablesInDatabase(spark, catalog, row.namespace)
    return tables

def getDescription(spark, name):
    rows = spark.sql(f'DESCRIBE FUNCTION EXTENDED {name}').collect()
    textLines = list(map(lambda r: r.function_desc, rows))
    return "\n".join(textLines)


def getFunctions(spark):
    spark.sql('USE spark_catalog')
    rows = spark.sql('SHOW FUNCTIONS').collect()
    return list(map(lambda f: {
        "name": f.function,
        "description": getDescription(spark, f.function)
    }, rows))


def getSparkDatabaseSchema(spark, catalogs):
    return {
        "functions": getFunctions(spark),
        "tables": getTablesInCatalogs(spark, catalogs)
    }


def checkAndUpdateSchema(spark, schemaFileName, refresh_threshold, catalogs):
    file_exists = path.isfile(schemaFileName)
    ttl_expired = False
    if file_exists:
        file_time = path.getmtime(schemaFileName)
        current_time = time.time()
        if current_time - file_time > refresh_threshold:
            print(f'TTL {refresh_threshold} seconds expired, re-generating schema file: {schemaFileName}')
            ttl_expired = True
    else:
        print(f'Generating schema file: {schemaFileName}')
        
    if (not file_exists) or ttl_expired:
        sparkdb_schema = getSparkDatabaseSchema(spark, catalogs)
        # Save schema to disk. sql-language-server will pickup any changes to this file.
        with open(schemaFileName, 'w') as fout:
            json.dump(sparkdb_schema, fout, sort_keys=True, indent=2)
        print('Schema file updated: ' + schemaFileName)
