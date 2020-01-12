import os
import sys
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal
import numpy as np
import requests
import pytest

sys.path.append('..')
from src.db import build_connection, reader, check_table, load_csv
from src.preprocessing import add_age_bins, prepare_chunk, handle_missing, process_chunk
from src.supplement import DRG_FORMAT_URL, download_file


def test_config_file_exists():
    config_path = 'config.json'
    exists = os.path.exists(config_path)
    is_file = os.path.isfile(config_path)
    assert exists and is_file


def test_reader(tmpdir):
    p = tmpdir.join('a_file.txt')
    p.write('This is a test')
    filename = os.path.join(p.dirname, p.basename)
    content = reader(filename)
    assert content == 'This is a test'


def test_data_files_exist():
    assert os.path.exists(
        'data/NRD_2016_Core.CSV') and os.path.isfile('data/NRD_2016_Core.CSV')
    assert os.path.exists(
        'data/NRD_2016_Hospital.CSV') and os.path.isfile('data/NRD_2016_Hospital.CSV')
    assert os.path.exists(
        'data/NRD_2016_Severity.CSV') and os.path.isfile('data/NRD_2016_Severity.CSV')


def test_check_table(cursor):
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS test.test_table (col1 INT, col2 INT)""")
    table_exists = check_table(cursor, 'test_table', 'test')
    assert table_exists


def test_load_csv(cursor, tmpdir):
    p = tmpdir.join('a_csv_file.csv')
    p.write('1,1\n2,2')
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS test.test_load (col1 INT, col2 INT)""")
    csv_file = os.path.join(p.dirname, p.basename)
    load_csv(cursor, csv_file, 'test.test_load')
    cursor.execute('SELECT * FROM test.test_load')
    check_data = [tuple(i) for i in cursor.fetchall()]
    assert check_data == [(1, 1), (2, 2)]


def test_download_file(tmpdir):
    # test downloading the file
    p = tmpdir.join('the_sas_file.txt')
    save_path = os.path.join(p.dirname, p.basename)
    download_file(DRG_FORMAT_URL, save_path)
    with open(save_path, 'r') as f:
        content = f.read()
    assert content != ''  # check if file was actually written to
    assert type(content) == str
    # check if the target patterns exist in the file
    assert 'Value DRGv33f /* DRG - version 33 */' in content
    assert 'Value DRGv34f /* DRG - version 34 */' in content
    assert 'Value DRG33MSF /* DRG - version 33 to Medical/Surgical Flag */' in content
    assert 'Value DRG34MSF /* DRG - version 34 to Medical/Surgical Flag */' in content


@pytest.fixture
def chunk():
    df = DataFrame({
        'age': [0, 4, 5, 6, 19, 37, 55, 73, 72, 54],
        'categorical_column': ['-6', '1', '2', '3', '4', '5', '6', '7', '8', '9'],
        'continuous_column': ['10', '20', '30', '40', '50', '60', '70', '80', '90', '-100'],
        'hosp_nrd': list('0123456789'),
        'key_nrd': list('9874123056')
    })
    return df


@pytest.fixture
def dtypes():
    data_types = {
        'age': 'int8',
        'categorical_column': 'int8',
        'continuous_column': 'int64',
    }
    return data_types


def test_prepare_chunk(chunk, dtypes):
    new_chunk = prepare_chunk(chunk, dtypes)
    assert 'key_nrd' not in new_chunk.columns
    assert 'hosp_nrd' not in new_chunk.columns
    assert new_chunk['age'].dtype == 'int8'
    assert new_chunk['categorical_column'].dtype == 'int8'
    assert new_chunk['continuous_column'].dtype == 'int64'


def test_add_age_bins(chunk, dtypes):
    new_chunk = chunk = chunk.pipe(
        prepare_chunk, dtypes=dtypes).pipe(add_age_bins)
    expected_columns = ['age', 'categorical_column', 'continuous_column',
                        '5-18', '19-36', '37-54', '55-72', '73+']
    assert new_chunk.columns.tolist() == expected_columns
    excpected_dtype_lst = ['int8', 'int8', 'int64',
                           'uint8', 'uint8', 'uint8', 'uint8', 'uint8']
    expected_dtypes = {k: v for k, v in zip(
        expected_columns, excpected_dtype_lst)}

    expected_data = [[0, -6, 10, 0, 0, 0, 0, 0],
                     [4, 1, 20, 1, 0, 0, 0, 0],
                     [5, 2, 30, 1, 0, 0, 0, 0],
                     [6, 3, 40, 1, 0, 0, 0, 0],
                     [19, 4, 50, 0, 1, 0, 0, 0],
                     [37, 5, 60, 0, 0, 1, 0, 0],
                     [55, 6, 70, 0, 0, 0, 1, 0],
                     [73, 7, 80, 0, 0, 0, 0, 1],
                     [72, 8, 90, 0, 0, 0, 1, 0],
                     [54, 9, -100, 0, 0, 1, 0, 0]]
    expected_chunk = DataFrame(
        data=expected_data, columns=expected_columns).astype(expected_dtypes)
    assert_frame_equal(expected_chunk, new_chunk)


def test_handle_missing(chunk, dtypes):
    new_chunk = chunk.pipe(prepare_chunk, dtypes=dtypes).pipe(handle_missing)
    expected_chunk = DataFrame({
        'age': [0, 4, 5, 6, 19, 37, 55, 73, 72, 54],
        'categorical_column': [-1, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        'continuous_column': [10, 20, 30, 40, 50, 60, 70, 80, 90, np.nan],
    }).astype({'age': 'int8', 'categorical_column': 'int8', 'continuous_column': 'float64'})
    assert new_chunk['age'].dtype == 'int8'
    assert new_chunk['categorical_column'].dtype == 'int8'
    assert new_chunk['continuous_column'].dtype == 'float64'
    assert_frame_equal(expected_chunk, new_chunk)


def test_process_chunk(chunk, dtypes):
    processed_chunk = process_chunk(chunk, dtypes)
    expected_columns = ['age', 'categorical_column', 'continuous_column', '5-18', '19-36',
                        '37-54', '55-72', '73+']
    assert processed_chunk.columns.tolist() == expected_columns
    expected_data = [[0., -1., 10., 0., 0., 0., 0., 0.],
                     [4., 1., 20., 1., 0., 0., 0., 0.],
                     [5., 2., 30., 1., 0., 0., 0., 0.],
                     [6., 3., 40., 1., 0., 0., 0., 0.],
                     [19., 4., 50., 0., 1., 0., 0., 0.],
                     [37., 5., 60., 0., 0., 1., 0., 0.],
                     [55., 6., 70., 0., 0., 0., 1., 0.],
                     [73., 7., 80., 0., 0., 0., 0., 1.],
                     [72., 8., 90., 0., 0., 0., 1., 0.],
                     [54., 9., np.nan, 0., 0., 1., 0., 0.]]
    expected_dtype_lst = ['int8', 'int8', 'float64',
                          'uint8', 'uint8', 'uint8', 'uint8', 'uint8']
    expected_dtypes = {k: v for k, v in zip(
        expected_columns, expected_dtype_lst)}
    expected_chunk = DataFrame(
        data=expected_data, columns=expected_columns).astype(expected_dtypes)
    assert_frame_equal(expected_chunk, processed_chunk)
