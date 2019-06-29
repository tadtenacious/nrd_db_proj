import os
import re
import requests
import pandas as pd

DRG_FORMAT_URL = 'https://www.hcup-us.ahrq.gov/db/tools/DRG_Formats.TXT'
DRG_FILE_NAME = 'DRG_Formats.TXT'
SAVE_PATH = os.path.join('data', DRG_FILE_NAME)


def download_file(url, save_path):
    with open(save_path, 'w') as f:
        resp = requests.get(url)
        f.write(resp.content)
    return


def main():
    if not os.path.exists('data'):
        os.mkdir('data')
    if not os.path.exists(SAVE_PATH):
        download_file(DRG_FORMAT_URL, SAVE_PATH)
    return


if __name__ == "__main__":
    main()
