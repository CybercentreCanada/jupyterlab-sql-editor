import json
import os.path as path
import time


MAX_RET = 20000

def getColumns(cur, tableName):
    sql = f'SHOW COLUMNS IN {tableName}'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
    return list(map(lambda r: {
        'columnName': r[0],
        'metadata': r[2],
        'type': r[1],
        'description': r[3]
    }, rows))


def getTables(cur, catalog, database):
    # prevent retrieving tables from information_schema
    if database == 'information_schema':
        return []
    
    path = f'{catalog}.{database}'
    sql = f'SHOW TABLES IN {path}'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
    tables = []
    if len(rows) > 0:
        for r in rows:
            if len(tables) > 50:
                break
            table = r[0]
            tables.append( {
                "tableName": table,
                "columns": getColumns(cur, f'{path}."{table}"'),
                "database": database,
                "catalog": catalog
            })
    return tables

def getSchemas(cur, catalog):
    sql = f'SHOW SCHEMAS IN {catalog}'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
    tables = []
    for r in rows:
        database = r[0]
        tables += getTables(cur, catalog, database)
    return tables

def getCatalogs(cur, catalogs):
    tables = []
    for catalog in catalogs:
        tables += getSchemas(cur, catalog)
    return tables

def getFunctions(cur):
    sql = 'SHOW FUNCTIONS'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
     # initialize a null list
    functions = []
    for r in rows:
        name = r[0]
        if not name in functions:
            functions.append(name)
    return list(map(lambda name: {
        "name": name,
        "description": ''
    }, functions))

def getDatabaseSchema(cur, catalogs):
    return {
        "tables": getCatalogs(cur, catalogs),
        "functions": getFunctions(cur)
    }

def updateDatabaseSchema(cur, schemaFileName, catalogs):
    db_schema = getDatabaseSchema(cur, catalogs)
    # Save schema to disk. sql-language-server will pickup any changes to this file.
    with open(schemaFileName, 'w') as fout:
        json.dump(db_schema, fout, sort_keys=True, indent=2)
    print('Schema file updated: ' + schemaFileName)

