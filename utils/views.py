import psycopg2
from .db import reader


def make_views(con, cursor):
    views = reader('sql/create_views.sql')
    print('Creating views. This will take a while.')
    try:
        cursor.execute(views)
        con.commit()
        print('Views created successfully.')
    except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
        print('An error occured.')
        print(e)
        con.rollback()
    return
