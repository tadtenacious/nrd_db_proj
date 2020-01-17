import os
import sys
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal
import numpy as np
import requests
import pytest

sys.path.append('..')
from src.db import build_connection, reader, check_table, load_csv
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
