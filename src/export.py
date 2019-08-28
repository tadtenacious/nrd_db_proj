def export(cursor, sample=False):
    '''A function to export the feature set from the database. 
    Requires the cursor and bloolean value for sample. sample=True exports 1% sample.'''
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
    print('Data export complete.')
    return
