import os
import sys
from pandas import DataFrame, Series
from numpy import nan
# import pytest

sys.path.append('..')
from src.db import build_connection, reader, check_table, load_csv
from src.preprocessing import fill_mean, fill_cat, preprocess


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


def test_fill_cat():
    df = DataFrame(
        {'col1': [1, 2, 3, -5], 'col2': [-4, -3, -2, 0]}).pipe(fill_cat)
    assert df.equals(
        DataFrame({'col1': [1.0, 2.0, 3.0, -1.0], 'col2': [-4, -3, -2, 0]}))


def test_fill_mean():
    # fill with means from dataframe
    df1 = DataFrame(
        {'col1': [-99, 10, 10], 'col2': [-98, 100, 100]}).pipe(fill_mean)
    assert df1.equals(DataFrame(
        {'col1': [10.0, 10.0, 10.0], 'col2': [-98, 100, 100]}))
    # fill with means from another source
    means = Series([9, 9], index=['col1', 'col2'])
    df2 = DataFrame(
        {'col1': [-99, 10, 10], 'col2': [-98, 100, 100]}).pipe(fill_mean, means=means)
    assert df2.equals(DataFrame(
        {'col1': [9.0, 10.0, 10.0], 'col2': [-98, 100, 100]}))


def test_preprocessing():
    df1 = DataFrame(
        {'col1': [100, 100, -99], 'col2': [1, -4, -5]}).pipe(preprocess)
    assert df1.equals(
        DataFrame({'col1': [100.0, 100.0, 100.0], 'col2': [1.0, -4.0, -1.0]}))
