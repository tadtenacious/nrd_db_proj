import os
import sys
import pytest


def test_config_file_exists():
    config_path = 'config.json'
    exists = os.path.exists(config_path)
    is_file = os.path.isfile(config_path)
    assert exists and is_file
