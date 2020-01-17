import getpass
import json
import psycopg2
# from sqlalchemy import create_engine


def build_connection(path, timeout=10):
    '''Build the connection to the postgresql databse.'''
    with open(path, 'r') as f:
        config = json.loads(f.read())
    pw = getpass.getpass('Please enter your password:\n')
    con = psycopg2.connect(dbname=config['dbname'], user=config['user'], host=config['host'],
                           password=pw, connect_timeout=timeout)
    return con


def build_con_string(path):
    with open(path, 'r') as f:
        config = json.loads(f.read())
    pw = getpass.getpass('Please enter your password:\n')
    con_string = 'postgresql://' + \
        config['user'] + ':' + pw + '@' + \
        config['host'] + ':5432' + '/' +config['dbname']
    return con_string


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


def load_csv(cursor, csv_path, target_table, sep=','):
    '''Load a csv to a table in postgresql.'''
    with open(csv_path, 'r') as f:
        cursor.copy_from(f, target_table, sep=sep, null='', size=819000)
    return
