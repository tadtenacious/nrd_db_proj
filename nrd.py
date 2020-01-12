import argparse
import os


def main(args):
    # given the config argument, set up configuration file
    if args.config:
        from src.config import config
        config()
        return
    has_config = os.path.exists(
        'config.json') and os.path.isfile('config.json')
    # if the configuration file does not exist run the set up anyway
    if not has_config:
        from src.config import config
        config()
    # given the etl argument, run the etl
    if args.etl:
        from src.etl import do_etl
        do_etl()
        return
    # given the export argument, export the sample or full data set
    if args.export:
        if args.export == 'sample':
            sample = True
        else:
            sample = False
        from src.export import export
        from src.db import build_connection
        con = build_connection('config.json')
        export(con, sample)
        con.close()
        return
    # given the model argument, run the model on the sample or full data set
    if args.model:
        if args.model == 'sample':
            fpath = 'data/feature_set_sample.csv'
        else:
            fpath = 'data/feature_set.csv'
        from src.model import run_model
        run_model(fpath)
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='HCUP NRD Command-line tool to set up connection to the database,'
        '\nperform ETL,\nexport the data from the database'
        '\nrun the model on a 1 percent sample or the full data set.'
    )
    parser.add_argument('--config', help='Set up the connection to the database.'
                        '\nRequires user name, host name, and database name.', action='store_true')
    parser.add_argument('--etl', help='Perform the ETL and export the full and sample data sets.'
                        ' This may take a while depending on your database\'s resources.',
                        action='store_true')
    parser.add_argument('--export', help='Export the sample or full data set',
                        choices=['sample', 'full'])
    parser.add_argument('--model', help='Run the model on the sample or full data set.',
                        choices=['sample', 'full'])
    args = parser.parse_args()
    main(args)
