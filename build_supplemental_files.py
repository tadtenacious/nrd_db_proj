import os
import re
import requests
import pandas as pd
from utils.sas import sas_file_to_dataframe

DRG_FORMAT_URL = 'https://www.hcup-us.ahrq.gov/db/tools/DRG_Formats.TXT'
DRG_FILE_NAME = 'DRG_Formats.TXT'
SAVE_PATH = os.path.join('data', DRG_FILE_NAME)


def download_file(url, save_path):
    with open(save_path, 'wb') as f:
        resp = requests.get(url)
        f.write(resp.content)
    return


def main():
    if not os.path.exists('data'):
        os.mkdir('data')
    if not os.path.exists(SAVE_PATH):
        download_file(DRG_FORMAT_URL, SAVE_PATH)

    with open(SAVE_PATH, 'r') as f:
        data = f.read()
    drg_name_cols = ['drg', 'drgName']
    v33_drg_names = sas_file_to_dataframe(
        data, 'Value DRGv33f /* DRG - version 33 */', drg_name_cols, '33')
    v34_drg_names = sas_file_to_dataframe(
        data, 'Value DRGv34f /* DRG - version 34 */', drg_name_cols, '34')
    drg_names = pd.concat([v33_drg_names, v34_drg_names],
                          ignore_index=True, sort=False)[['drgver', 'drg', 'drgName']]
    # clean the data to match 3 digit DRG codes
    drg_names['drg'] = drg_names['drg'].apply(
        lambda x: x if x == '.' else x.zfill(3))
    # clean the name field
    drg_names['drgName'] = drg_names['drgName'].str.replace(
        r'[ 0-9.]+:', '').str.strip()
    drg_names.to_csv('data/lu_drg_names.csv', index=False, header=False)

    return


if __name__ == "__main__":
    main()
