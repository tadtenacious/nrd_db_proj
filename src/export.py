import json
import os
import numpy as np
from pandas import read_sql, Series
from .preprocessing import process_chunk
from dask import delayed
import dask.dataframe as dd
from multiprocessing import cpu_count, Pool


def write_chunk(chunk, dtypes, file_name):
    # chunk.pipe(process_chunk, dtypes=dtypes).to_parquet(
    #     file_name, engine='fastparquet', compression='snappy')
    chunk.pipe(process_chunk, dtypes=dtypes).to_csv(file_name, index=False)
    return


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
    if sample:
        chunks = read_sql(query, connection, chunksize=20000)
        frames = [delayed(process_chunk)(chunk=chunk, dtypes=dtypes)
                  for chunk in chunks]
        ddf = dd.from_delayed(frames)
        ddf.to_parquet(file_name, engine='fastparquet',
                       compression='snappy', write_index=False)
    else:
        os.mkdir('temp')
        nprocs = cpu_count() - 1
        pool = Pool(nprocs)
        chunks = read_sql(query, connection, chunksize=20000)
        out = [pool.apply_async(write_chunk, args=(
            chunk, dtypes, f'temp/part{i}.csv'),) for i, chunk in enumerate(chunks, 1)]
        pool.close()
        # ddf = dd.read_sql_table('feature_set', connection, index_col='key_nrd')
        # ddf = ddf.astype(dtypes).drop('hosp_nrd', axis=1)
        # age_labels = ['0-3', '5-18', '19-36', '37-54', '55-72', '73+']
        # age_bins = [-1, 4, 19, 37, 55, 73, 100]
        # ddf['age_bins'] = ddf['age'].map_partitions(
        #     cut, bins=age_bins, labels=age_labels)
        # for i in age_labels[1:]:
        #     ddf[i] = ddf['age_bins'].apply(lambda x, val=i: 1 if
        #                                    x == val else 0, meta=Series(dtype='int8', name=i))
        # ddf = ddf.drop('age_bins', axis=1)
        # cat_replace = ddf.gt(-99).lt(-4)
        # ddf = ddf.mask(cat_replace, -1)
        # nulls = ddf.lt(-98)

        # ddf = ddf.mask(nulls, np.NaN)

        # ddf.to_parquet(file_name, engine='fastparquet',
        #                compression='snappy', write_index=False)
    print('Data export complete.')
    return
