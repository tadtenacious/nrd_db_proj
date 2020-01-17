from numpy import nan
from pandas import cut, get_dummies
import json


def prepare_chunk(chunk):
    '''Prepare the dataframe chunk by converting the datatypes and dropping the
    unique identifiers, key_nrd and hosp_nrd.
    '''
    chunk = chunk.drop(['key_nrd', 'hosp_nrd'], axis=1)
    float_cols = [
        'los',
        'pl_nchs',
        'prday1',
        'prday2',
        'prday3',
        'prday4',
        'prday5',
        'prday6',
        'prday7',
        'prday8',
        'prday9',
        'prday10',
        'prday11',
        'prday12',
        'prday13',
        'prday14',
        'prday15',
        'totchg',
        'pir_totalchg',
        'pif_totalchg'
    ]
    for col in float_cols:
        chunk[col] = chunk[col].astype('float64')
    return chunk


def handle_missing(chunk):
    '''A function to handle or set missing values. Categorical variables that
     are missing have values between -98 and -5. These are all set to -1. As
     they are all the same values, they do not need to be set during the CV
     split. Continuous variables have missing values that are less than or 
     equal to -99. This will be set to nan and be filled during the CV split
     to avoid data leakage.
    '''
    categorical_missing = chunk.ge(-98) & chunk.le(-5)
    chunk = chunk.mask(categorical_missing, -1)
    nulls = chunk.le(-99)
    chunk = chunk.mask(nulls, nan)
    return chunk


def add_age_bins(chunk):
    '''Add the age bin features but binning the ages and the one hot encoding 
    them.
    '''
    age_labels = ['0-3', '5-18', '19-36', '37-54', '55-72', '73+']
    age_bins = [0, 4, 19, 37, 55, 73, 90]
    chunk = chunk.assign(age_bins=cut(chunk['age'], age_bins, right=False,
                                      labels=age_labels))
    for lbl in age_labels[1:]:
        chunk[lbl] = chunk['age_bins'].apply(
            lambda x, val=lbl: 1 if x == val else 0).astype('uint8')
    chunk = chunk.drop('age_bins', axis=1)
    # dummies = get_dummies(chunk['age_bins'], drop_first=True)
    # chunk = chunk.join(dummies).drop('age_bins', axis=1)
    return chunk


def process_chunk(chunk):
    '''A function to run all initial preprocessing steps.'''
    chunk = prepare_chunk(chunk)
    chunk = add_age_bins(chunk)
    chunk = handle_missing(chunk)

    # chunk = chunk.pipe(prepare_chunk).pipe(add_age_bins)\
    #     .pipe(handle_missing).astype('float64')
    return chunk
