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

def getCatalogs(cur):
    sql = f'SHOW CATALOGS'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
    tables = []
    for r in rows:
        catalog = r[0]
        if not catalog == 'system':
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

def getDatabaseSchema(cur):
    return {
        "tables": getCatalogs(cur),
        "functions": getFunctions(cur)
    }

def checkAndUpdateSchema(cur, schemaFileName, refresh_threshold):
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
        db_schema = getDatabaseSchema(cur)
        # Save schema to disk. sql-language-server will pickup any changes to this file.
        with open(schemaFileName, 'w') as fout:
            json.dump(db_schema, fout, sort_keys=True, indent=2)
        print('Schema file updated: ' + schemaFileName)

