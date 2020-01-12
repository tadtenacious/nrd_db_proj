import json
from pandas import read_sql
from .preprocessing import process_chunk
from dask import delayed
import dask.dataframe as dd


def export(connection, sample=False):
    '''A function to export the feature set from the database. 
    Requires the cursor and bloolean value for sample. sample=True exports 1% sample.'''
    select = 'SELECT * FROM feature_set'
    table_sample = " WHERE substring(key_nrd,8,1) = '1' and substring(key_nrd,9,1)='3'"
    if sample:
        query = select + table_sample
        file_name = 'data/sample_data.parquet'
    else:
        query = select
        file_name = 'data/full_data.parquet'
    if sample:
        msg = 'Exporting 1% sample.'
    else:
        msg = 'Exporting full dataset. This may take a while.'
    print(msg)
    with open('data/dtypes.json', 'r') as f:
        dtypes = json.loads(f.read())
    chunks = read_sql(query, connection, chunksize=20000)
    frames = [delayed(process_chunk)(chunk=chunk, dtypes=dtypes)
              for chunk in chunks]
    ddf = dd.from_delayed(frames)
    ddf.to_parquet(file_name, engine='fastparquet',
                   compression='snappy', write_index=False)
    print('Data export complete.')
    return
