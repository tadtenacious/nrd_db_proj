import json
import psycopg2


def build_connection(path):
    with open(path, 'r') as f:
        config = json.loads(f.read())
    con = psycopg2.connect(dbname=config['dbname'], user=config['user'], host=config['host'],
                           password=config['password'])
    return con


def check_table(cursor, table, schema='public'):
    '''Check if a table exist in postgresql.'''
    check = """SELECT EXISTS (
    SELECT 1
    FROM   information_schema.tables
    WHERE  table_schema = '{}'
    AND    table_name = '{}'
    )""".format(schema, table)

    cursor.execute(check)
    res = cursor.fetchall()[0][0]
    return res


def reader(path):
    with open(path, 'r') as f:
        q = f.read()
    return q


def load_csv(cursor, csv_path, target_table):
    '''Load a csv to a table in postgresql.'''
    with open(csv_path, 'r') as f:
        cursor.copy_from(csv_path, sep=',', null='', size=819000)
    return
