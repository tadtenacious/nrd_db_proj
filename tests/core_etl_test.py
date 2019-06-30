import os
import sys
import psycopg2
from pandas import read_csv

sys.path.append(os.path.abspath('..'))


def make_sample(in_path, new_path):
    '''Make a sample csv with 1,000 rows'''
    sample = read_csv(in_path, header=None, nrows=1000)
    sample.to_csv(new_path, index=False)
    return


def main():
    from utils.db import build_connection, check_table, reader, load_csv
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
    csv_to_table = {
        'raw_core': '../data/SAMPLE_NRD_2016_Core.CSV',
        'raw_hospital': '../data/SAMPLE_NRD_2016_Hospital.CSV',
        'raw_severity': '../data/SAMPLE_NRD_2016_Severity.CSV'
    }
    print('Creating sample files.')
    for in_file, new_file in csv_files.items():
        try:
            make_sample(in_file, new_file)
        except FileNotFoundError as e:
            print('Please put the HCUP CSV files in the data directory')
            print(e)
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
                file_to_load = csv_to_table[table]
                print('loading {}'.format(table))
                load_csv(cursor, file_to_load, table)
                con.commit()
            else:
                print('Failed creating {}'.format())
        except psycopg2.OperationalError as e:
            print('Failed creating {}'.format(table))
        print('Dropping {}'.format(table))
        cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
    con.close()
    print('Removing Sample Files')
    for sample_file in csv_files.values():
        os.remove(sample_file)


if __name__ == "__main__":
    main()
