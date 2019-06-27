import os
import sys
import psycopg2
from pandas import read_csv

sys.path.append(os.path.abspath('..'))


def make_sample(in_path, new_path):
    '''Make a sample csv with 1,000 rows'''
    sample = read_csv(in_path, header=None, nrows=1000)
    sample.to_csv(new_path)
    return


def main():
    from utils.db import build_connection, check_table, reader
    print('Reading sql files...')
    core = reader('../sql/create_raw_core.sql')
    hosp = reader('../sql/create_raw_hospital.sql')
    severity = reader('../sql/create_raw_severity.sql')
    tables = {
        'raw_core': core,
        'raw_hospital': hosp,
        'raw_severity': severity
    }
    csv_files = {
        '../data/NRD_2016_Core.CSV': '../data/SAMPLE_NRD_2016_Core.CSV',
        '../data/NRD_2016_Hospital.CSV': '../data/SAMPLE_NRD_2016_Hospital.CSV',
        '../data/NRD_2016_Severity.CSV': '../data/SAMPLE_NRD_2016_Severity.CSV'
    }
    for in_file, new_file in csv_files.items():
        make_sample(in_file, new_file)
    print('Connecting to server...')
    try:
        con = build_connection('../config.json')
        print('Connection to server successful!')
        cursor = con.cursor()
    except psycopg2.OperationalError as e:
        print('Connection to server unsuccesful.\nRerun setp_config.py')
        print(e)
    for table, statement in tables.items():
        print('Creating {}'.format(table))
        try:
            cursor.execute(statement)
            con.commit()
            check = check_table(cursor, table)
            if check:
                print('Successfully created {}'.format(table))
                cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
                con.commit()
            else:
                print('Failed creating {}'.format())
        except psycopg2.OperationalError as e:
            print('Failed creating {}'.format(table))
    con.close()


if __name__ == "__main__":
    main()
