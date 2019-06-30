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
    try:
        core = reader('../sql/create_raw_core.sql')
        hosp = reader('../sql/create_raw_hospital.sql')
        severity = reader('../sql/create_raw_severity.sql')
        # tables created entirely in SQL
        readmit = reader('../sql/create_readmit_core.sql')
        target = reader('../sql/create_target_table.sql')
        nrd_core = reader('../sql/create_nrd_core.sql')
    except FileNotFoundError as e:
        print('Cannot run tests without sql files. Try to download sql files at https://github.com/tadtenacious/nrd_db_proj/tree/master/sql')
        print('Or git clone https://github.com/tadtenacious/nrd_db_proj.git')
        print('Make sure you are running tests inside "tests" directory.')
        print(e)
        return

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
            return
    print('Connecting to server...')
    print('Running Tests.')
    total_tests = 0
    failed_tests = []

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
                load_csv(cursor, file_to_load, table)
                total_tests += 1
                con.commit()
            else:
                failed_tests.append('Loading {}'.format(table))
                print('Failed creating {}'.format(table))
                total_tests += 1
        except psycopg2.OperationalError as e:
            print('Failed creating {}'.format(table))
            failed_tests.append('Create {}'.format(table))
        drop_tables.append(table)
        total_tests += 1
    try:
        cursor.execute(readmit)
        con.commit()
        check_readmit = check_table(cursor, 'readmit_core')
        if not check_readmit:
            failed_tests.append('Create readmit_core')
    except psycopg2.OperationalError as e:
        failed_tests.append('Create readmit_core')
        print('Failed creating readmit_core')
        print(e)
    total_tests += 1
    drop_tables.append('readmit_core')
    try:
        cursor.execute(target)
        con.commit()
        check_target = check_table(cursor, 'target_table')
        if not check_target:
            failed_tests.append('Create target_table')
    except psycopg2.OperationalError as e:
        print('Failed creating target_table')
        print(e)
        failed_tests.append('Create target_table')
    total_tests += 1
    try:
        cursor.execute(nrd_core)
        con.commit()
        check_target = check_table(cursor, 'nrd_core')
        if not check_target:
            failed_tests.append('Create nrd_core')
    except psycopg2.OperationalError as e:
        print('Failed creating nrd_core')
        print(e)
        failed_tests.append('Create nrd_core')
    total_tests += 1

    drop_tables.append('nrd_core')
    print('Dropping tables.')
    for table in drop_tables:
        try:
            cursor.execute('DROP TABLE IF EXISTS {}'.format(table))
        except psycopg2.OperationalError as e:
            print(e)
    con.commit()
    con.close()
    print('Removing Sample Files')
    for sample_file in csv_files.values():
        os.remove(sample_file)
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
