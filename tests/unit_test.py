import os
import sys
# import pytest

sys.path.append('..')
from src.db import build_connection, reader, check_table, load_csv


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
