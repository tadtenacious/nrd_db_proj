from src.db import build_connection
import argparse


def export(sample=False):
    con = build_connection('config.json')
    cursor = con.cursor()
    select = 'SELECT * FROM feature_set'
    table_sample = " WHERE substring(key_nrd,8,1) = '1' and substring(key_nrd,9,1)='3'"
    if sample:
        query = select + table_sample
        file_name = 'data/feature_set_sample.csv'
    else:
        query = select
        file_name = 'data/feature_set.csv'
    output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
    if sample:
        msg = 'Exporting 1% sample.'
    else:
        msg = 'Exporting full dataset. This may take a while.'
    print(msg)
    with open(file_name, 'w') as f:
        cursor.copy_expert(output, f)
    con.close()
    print('Data export complete.')
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Export data from postgresql server')
    parser.add_argument(
        '--sample', help='Option to export 1% sample', action='store_true')
    args = parser.parse_args()
    sample = False
    if args.sample:
        sample = True
    export(sample)
