import os
import psycopg2
from .db import check_table, reader, load_csv


def etl(con, cursor):
    try:
        # core csv files
        core = reader('sql/create_raw_core.sql')
        hosp = reader('sql/create_raw_hospital.sql')
        severity = reader('sql/create_raw_severity.sql')
        # look up files
        lu_msf = reader('sql/create_lu_drg_MedSurgFlag.sql')
        lu_drg_names = reader('sql/create_lu_drg_names.sql')
        lu_mdc_names = reader('sql/create_lu_mdc_names.sql')
        # tables created entirely in SQL
        readmit = reader('sql/create_readmit_core.sql')
        target = reader('sql/create_target_table.sql')
        nrd_core = reader('sql/create_nrd_core.sql')
        update_nrd_core_drg = reader('sql/update_nrd_core_drg.sql')
    except FileNotFoundError as e:
        print('SQL files not found. Try to download sql files at https://github.com/tadtenacious/nrd_db_proj/tree/master/sql')
        print(e)
        return

    tables_to_create = {
        'raw_core': core,
        'raw_hospital': hosp,
        'raw_severity': severity,
        'lu_drg_msf': lu_msf,
        'lu_drg_names': lu_drg_names,
        'lu_mdc_names': lu_mdc_names,
        'readmit_core': readmit,
        'target_table': target,
        'nrd_core': nrd_core
    }

    tables_to_csv = {
        'raw_core': 'data/NRD_2016_Core.CSV',
        'raw_hospital': 'data/NRD_2016_Hospital.CSV',
        'raw_severity': 'data/NRD_2016_Severity.CSV',
        'lu_drg_msf': 'data/lu_drg_MedSurgFlag.csv',
        'lu_drg_names': 'data/lu_drg_names.csv',
        'lu_mdc_names': 'data/lu_mdc_names.csv'
    }

    flatfile_tables = [
        'raw_core',
        'raw_hospital',
        'raw_severity',
        'lu_drg_msf',
        'lu_drg_names',
        'lu_mdc_names'
    ]

    sql_tables = [
        'readmit_core',
        'target_table',
        'nrd_core'
    ]

    print('Loading flat files to sql. This may take a while.')
    for table in flatfile_tables:
        create_statement = tables_to_create[table]
        try:
            cursor.execute(create_statement)
            con.commit()
        except psycopg2.OperationalError as e:
            print('Error creating {}'.format(table))
            print(e)
            con.rollback()
            return
        try:
            csv_path = tables_to_csv[table]
            sep = ','
            if table.startswith('lu'):
                sep = '|'
            load_csv(cursor, csv_path, table, sep)
            con.commit()
        except psycopg2.OperationalError as e:
            print('Error loading {} from {}'.format(table, csv_path))
            print(e)
            con.rollback()
            return

    print('Creating tables to make the target column.')
    for table in sql_tables:
        create_statement = tables_to_create[table]
        try:
            cursor.execute(create_statement)
            con.commit()
        except psycopg2.OperationalError as e:
            print('Error creating {}'.format(table))
            print(e)
            con.rollback()
            return
    print('Updating drg column in nrd_core.')
    try:
        cursor.execute(update_nrd_core_drg)
        con.commit()
    except psycopg2.OperationalError as e:
        print('Error updating nrd_core drg column.')
        print(e)
        con.rollback()
    print('Initial ETL completed.')
    return
