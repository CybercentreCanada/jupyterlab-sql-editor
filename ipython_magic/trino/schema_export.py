import json

from trino.exceptions import TrinoUserError


MAX_RET = 20000


def get_columns(cur, table_name):
    try:
        sql = f'SHOW COLUMNS IN {table_name}'
        #print(sql)
        cur.execute(sql)
        rows = cur.fetchmany(MAX_RET)
        return list(map(lambda r: {
            'columnName': r[0],
            'metadata': r[2],
            'type': r[1],
            'description': r[3]
        }, rows))
    except TrinoUserError:
        print(f'Failed to get columns for {table_name}')
        return []


def get_tables(cur, catalog, database):
    # prevent retrieving tables from information_schema
    if database == 'information_schema':
        return []
    path = f'{catalog}.{database}'
    try:
        sql = f'SHOW TABLES IN {path}'
        #print(sql)
        cur.execute(sql)
        rows = cur.fetchmany(MAX_RET)
        tables = []
        if len(rows) > 0:
            for row in rows:
                if len(tables) > 50:
                    break
                table = row[0]
                tables.append( {
                    "table_name": table,
                    "columns": get_columns(cur, f'{path}."{table}"'),
                    "database": database,
                    "catalog": catalog
                })
        return tables
    except TrinoUserError:
        print(f'Failed to get tables for {path}')
        return []


def get_schemas(cur, catalog):
    sql = f'SHOW SCHEMAS IN {catalog}'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
    tables = []
    for row in rows:
        database = row[0]
        tables += get_tables(cur, catalog, database)
    return tables


def get_catalogs(cur, catalogs):
    tables = []
    for catalog in catalogs:
        tables += get_schemas(cur, catalog)
    return tables


def get_functions(cur):
    sql = 'SHOW FUNCTIONS'
    #print(sql)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_RET)
     # initialize a null list
    functions = []
    for row in rows:
        name = row[0]
        if not name in functions:
            functions.append(name)
    return list(map(lambda name: {
        "name": name,
        "description": ''
    }, functions))


def get_database_schema(cur, catalogs):
    return {
        "tables": get_catalogs(cur, catalogs),
        "functions": get_functions(cur)
    }


def update_database_schema(cur, schema_file_name, catalogs):
    db_schema = get_database_schema(cur, catalogs)
    # Save schema to disk. sql-language-server will pickup any changes to this file.
    with open(schema_file_name, 'w', encoding="utf8") as fout:
        json.dump(db_schema, fout, sort_keys=True, indent=2)
    print('Schema file updated: ' + schema_file_name)
