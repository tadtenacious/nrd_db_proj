import os
import sys
import psycopg2
from pandas import read_csv

sys.path.append(os.path.abspath('..'))


def main():
    from utils.db import build_connection, check_table, reader, load_csv
    try:
        lu_msf = reader('../sql/create_lu_drg_MedSurgFlag.sql')
        lu_drg_names = reader('../sql/create_lu_drg_names.sql')
        lu_mdc_names = reader('../sql/create_lu_mdc_names.sql')
    except FileNotFoundError as e:
        print('Cannot run tests without sql files. Try to download sql files at https://github.com/tadtenacious/nrd_db_proj/tree/master/sql')
        print('Or git clone https://github.com/tadtenacious/nrd_db_proj.git')
        print('Make sure you are running tests inside "tests" directory.')
        print(e)
        return

    tables = {
        'lu_drg_msf': lu_msf,
        'lu_drg_names': lu_drg_names,
        'lu_mdc_names': lu_mdc_names
    }
    csv_to_table = {
        'lu_drg_msf': '../data/lu_drg_MedSurgFlag.csv',
        'lu_drg_names': '../data/lu_drg_names.csv',
        'lu_mdc_names': '../data/lu_mdc_names.csv'
    }
    total_tests = 0
    failed_tests = []
    print('Running Tests.')
    try:
        con = build_connection('../config.json')
        cursor = con.cursor()
    except psycopg2.OperationalError as e:
        print('Connection to server unsuccesful.\nRerun setp_config.py')
        print(e)
        print('\nCannot run tests without successful connection to server.')
        return
    drop_tables = []
    for table, statement in tables.items():
        try:
            cursor.execute(statement)
            con.commit()
            check = check_table(cursor, table)
            if check:
                file_to_load = csv_to_table[table]
                load_csv(cursor, file_to_load, table, sep='|')
                total_tests += 1
                con.commit()
            else:
                failed_tests.append('Loading {}'.format(table))
                print('Failed creating/loading {}'.format(table))
                total_tests += 1
        except (psycopg2.OperationalError, psycopg2.DataError) as e:
            print('Failed loading creating {}'.format(table))
            print(e)
            failed_tests.append('Create {}'.format(table))
        drop_tables.append(table)
        total_tests += 1
    print('Dropping tables.')
    for table in drop_tables:
        try:
            cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
        except psycopg2.OperationalError as e:
            print(e)
    con.close()
    print('>>>Summary<<<')
    num_failed = len(failed_tests)
    print('\tTotal tests run: {}'.format(total_tests))
    print('\tSuccessful tests: {}'.format(total_tests - num_failed))
    print('\tFailed tests: {}'.format(num_failed))
    if num_failed > 0:
        for test in failed_tests:
            print('\t' + test)


if __name__ == "__main__":
    main()
