import psycopg2
from utils.db import build_connection, reader


def main():
    con = build_connection('config.json')
    views = reader('sql/create_views.sql')
    try:
        cursor = con.cursor()
        cursor.execute(views)
        con.commit()
        print('Script completed successfully.')
    except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
        print('An error occured.')
        print(e)
        con.rollback()
    con.commit()
    return


if __name__ == "__main__":
    main()
