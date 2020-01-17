import json
import os
import shutil
from numpy import nan
from pandas import DataFrame, Series, read_sql
import dask.dataframe as dd


def iter_many(cursor, columns, size=50000, dtypes=None):
    '''A generator to iterate over rows of a psycopg2 cursor. Psycopg2 or 
    PostgreSQL require a named cursor in order to have a server side cursor. 
    Otherwise the entire query result set is loaded into memory. 

    Parameters
    ----------
    cursor: a cursor from the psycopg2 connection object that is named.
    columns: a list columns from query required for manual dataframe constructor.
    size: integer of how many rows to return per iteration, default 50,000.
    dtypes: a dictionary of columns to pandsa datatypes, default None.
    '''
    while True:
        records = cursor.fetchmany(size=size)
        if not records:
            break
        if dtypes:
            df = DataFrame(data=records, columns=columns).astype(dtypes)
        else:
            df = DataFrame(data=records, columns=columns)
        yield df


def export(connection, sample=False):
    '''A function to export the feature set from the database. 
    Requires the connection and bloolean value for sample. sample=True exports 1% sample.'''
    if sample:
        query = 'SELECT * FROM feature_set_sample'
        file_name = 'data/sample_data'
        msg = 'Exporting 1% sample.'
        cursor_name = 'fetchsample'
    else:
        query = 'SELECT * FROM feature_set'
        file_name = 'data/full_data'
        msg = 'Exporting full dataset. This may take a while.'
        cursor_name = 'fetchfull'
    print(msg)

    with open('data/dtypes.json', 'r') as f:
        dtypes = json.loads(f.read())

    feature_cols = list(dtypes.keys())

    if not os.path.exists('temp'):
        os.mkdir('temp')

    cursor = connection.cursor(name=cursor_name)  # creates server side cursor
    cursor.execute(query)
    chunks = iter_many(cursor, columns=feature_cols,
                       size=100000, dtypes=dtypes)
    for i, chunk in enumerate(chunks, 1):
        tpath = os.path.join('temp', f'prefeature{i}.parquet')
        chunk.to_parquet(tpath, index=False, engine='pyarrow',
                         compression='snappy')

    df = dd.read_parquet('temp', engine='pyarrow')
    cat_replace = df.gt(-99) & df.lt(-4)
    df = df.mask(cat_replace, -1)
    nulls = df.lt(-98)
    df = df.mask(nulls, nan)
    df.to_parquet(file_name, engine='pyarrow',
                  compression='snappy', write_index=False)
    shutil.rmtree('temp')
    print('Data export complete.')
    return
