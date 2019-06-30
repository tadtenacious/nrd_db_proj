import os
import re
import requests
from pandas import concat
from utils.sas import sas_file_to_dataframe

DRG_FORMAT_URL = 'https://www.hcup-us.ahrq.gov/db/tools/DRG_Formats.TXT'
DRG_FILE_NAME = 'DRG_Formats.TXT'
SAVE_PATH = os.path.join('data', DRG_FILE_NAME)

mdc_short_map = {
    "ALCOHOL/DRUG USE & ALCOHOL/DRUG INDUCED ORGANIC MENTAL DISORDERS": "ALCOHOL/DRUG",
    "DISEASES & DISORDERS OF THE RESPIRATORY SYSTEM": "RESPIRATORY",
    "MULTIPLE SIGNIFICANT TRAUMA": "MULTIPLE TRAUMA",
    "DISEASES & DISORDERS OF THE HEPATOBILIARY SYSTEM & PANCREAS": "HEPATOBILIARY",
    "DISEASES & DISORDERS OF THE MALE REPRODUCTIVE SYSTEM": "MALE REPRO",
    "DISEASES & DISORDERS OF THE KIDNEY & URINARY TRACT": "KIDNEY & URINARY",
    "INFECTIOUS & PARASITIC DISEASES, SYSTEMIC OR UNSPECIFIED SITES": "INFECT & PARASITIC",
    "ENDOCRINE, NUTRITIONAL & METABOLIC DISEASES & DISORDERS": "ENDOCRINE",
    "DISEASES & DISORDERS OF THE CIRCULATORY SYSTEM": "CIRCULATORY",
    "DISEASES & DISORDERS OF THE SKIN, SUBCUTANEOUS TISSUE & BREAST": "SKIN",
    "DISEASES & DISORDERS OF THE FEMALE REPRODUCTIVE SYSTEM": "FEMALE REPRO",
    "BURNS": "BURNS",
    "DISEASES & DISORDERS OF THE EYE": "EYE",
    "NEWBORNS & OTHER NEONATES WITH CONDTN ORIG IN PERINATAL PERIOD": "NEWBORNS",
    "DISEASES & DISORDERS OF BLOOD, BLOOD FORMING ORGANS, IMMUNOLOG DISORD": "BLOOD",
    "INJURIES, POISONINGS & TOXIC EFFECTS OF DRUGS": "POISONINGS",
    "MISSING": "MISSING",
    "MYELOPROLIFERATIVE DISEASES & DISORDERS, POORLY DIFFERENTIATED NEOPLASM": "MYELOPROLIFERATIVE",
    "DISEASES & DISORDERS OF THE DIGESTIVE SYSTEM": "DIGESTIVE",
    "PRINCIPAL DX CANNOT BE ASSIGNED TO MDC": "NOT ASSIGNED",
    "DISEASES & DISORDERS OF THE EAR, NOSE, MOUTH & THROAT": "ENTM",
    "HUMAN IMMUNODEFICIENCY VIRUS INFECTIONS": "HIV",
    "DISEASES & DISORDERS OF THE NERVOUS SYSTEM": "NERVOUS",
    "PREGNANCY, CHILDBIRTH & THE PUERPERIUM": "PREGNANCY",
    "FACTORS INFLUENCING HLTH STAT & OTHR CONTACTS WITH HLTH SERVCS": "HLTH SERVCS",
    "DISEASES & DISORDERS OF THE MUSCULOSKELETAL SYSTEM & CONN TISSUE": "MUSCULOSKELETAL",
    "MENTAL DISEASES & DISORDERS": "MENTAL"
}


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
    drg_names = concat([v33_drg_names, v34_drg_names],
                       ignore_index=True, sort=False)
    # clean the data to match 3 digit DRG codes
    drg_names['drg'] = drg_names['drg'].apply(
        lambda x: x if x == '.' else x.zfill(3))
    # clean the name field
    drg_names['drgName'] = drg_names['drgName'].str.replace(
        r'[ 0-9.]+:', '').str.strip()
    drg_names.to_csv('data/lu_drg_names.csv',
                     index=False, header=False, sep='|')

    msf_cols = ['drg', 'msf']
    v33_msf = sas_file_to_dataframe(
        data, 'Value DRG33MSF /* DRG - version 33 to Medical/Surgical Flag */', msf_cols, '33')
    v34_msf = sas_file_to_dataframe(
        data, 'Value DRG34MSF /* DRG - version 34 to Medical/Surgical Flag */', msf_cols, '34')
    lu_msf = concat([v33_msf, v34_msf], ignore_index=True, sort=False)
    lu_msf.to_csv('data/lu_drg_MedSurgFlag.csv',
                  index=False, header=False, sep='|')

    mdc_cols = ['mdc', 'mdc_name']
    v33_mdc = sas_file_to_dataframe(
        data, 'Value MDCv33f /* MDC - version 33 */', mdc_cols, '33')
    v34_mdc = sas_file_to_dataframe(
        data, 'Value MDCv34f /* MDC - version 34 */', mdc_cols, '34')
    lu_mdc = concat([v33_mdc, v34_mdc], ignore_index=True, sort=False,)
    # clean the mdcn_name field
    lu_mdc['mdc_name'] = lu_mdc['mdc_name'].str.replace(
        r'[ 0-9.]+:', '').str.strip()
    lu_mdc['mdc_short'] = lu_mdc['mdc_name'].map(mdc_short_map)
    lu_mdc.to_csv('data/lu_mdc_names.csv', index=False, header=False, sep='|')

    return


if __name__ == "__main__":
    main()
