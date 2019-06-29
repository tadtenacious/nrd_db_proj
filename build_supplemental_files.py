import os
import re
import requests
import pandas as pd

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
    s = re.search(
        r'Value DRG33MSF /\* DRG - version 33 to Medical/SurgicalFlag \*/.*\s+[;]', data, re.DOTALL).string

    return


if __name__ == "__main__":
    main()
