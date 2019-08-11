import pytest
import sys
sys.path.append('..')

from src.db import build_connection


@pytest.fixture(scope='session')
def con():
    con = build_connection('config.json')
    yield con
    con.close()


@pytest.fixture
def cursor(con):
    cursor = con.cursor()
    cursor.execute("""CREATE SCHEMA IF NOT EXISTS test""")
    yield cursor
    cursor.execute("""DROP SCHEMA IF EXISTS test CASCADE""")
    con.rollback()
