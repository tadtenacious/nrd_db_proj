import psycopg2

from utils.db import build_connection
from utils.etl import etl
from utils.supplement import build_supplemental_files
from utils.views import make_views


def main():
    # create the supplemental files
    build_supplemental_files()
    try:
        con = build_connection()
        cursor = con.cursor()
    except psycopg2.OperationalError as e:
        print('An error occured connecting to the server. Please check your configuration file.')
        print(e)
        return
    etl(con, cursor)
    make_views(con, cursor)
    con.close()
    return


if __name__ == "__main__":
    main()
