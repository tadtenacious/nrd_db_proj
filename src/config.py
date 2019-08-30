import getpass
import json
import os

import psycopg2


def check_for_config():
    if os.path.exists('config.json'):
        return True
    else:
        return False


def create_config():
    user = input('Enter database User Name: ')
    host = input('Enter Host name: ')
    dbname = input('Enter Database Name: ')
    config_dict = {
        'user': user,
        'host': host,
        'dbname': dbname
    }
    with open('config.json', 'w') as f:
        json.dump(config_dict, f)
    print('Saved configuration file to config.json')
    return config_dict


def test_connect(config):
    user = config['user']
    pw = getpass.getpass('Please enter your password:\n')
    host = config['host']
    test = False
    try:
        con = psycopg2.connect(dbname='postgres', user=user,
                               host=host, password=pw)
        con.close()
        print('Successfully connected')
        test = True
    except psycopg2.OperationalError as e:
        print(e)
    return test


def config():
    check = check_for_config()
    if check:
        overwrite = input(
            'Configuration file exists, write over existing file (Y/N)? ')
        if overwrite.upper() == 'Y':
            config = create_config()
        else:
            with open('config.json', 'r') as f:
                config = json.loads(f.read())
    else:
        config = create_config()
    print('Ready to test configuration. Make sure the server is running.')
    run_test = input('Ready to test connection to server (Y/N)? ')
    if run_test.upper() == 'Y':
        test = test_connect(config)
        if test:
            print('Connection to server successful. You are ready to run the tests.')
        else:
            print(
                'Connection to server failed. Please check credentials and host location.')
            print('Make sure the server is running and rerun this script')
    return


if __name__ == "__main__":
    config()
