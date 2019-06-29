import re
from pandas import DataFrame


def get_section(sas_string, format_name):
    '''Parse the specific HCUP SAS files that holds the DRG tables.'''
    new_pattern = format_name.replace('*', '\*') + '[^;]*'
    search = re.search(new_pattern, sas_string, re.DOTALL)
    if search:
        return search.group(0)
    else:
        return None


def get_values(sas_section):
    '''Parse SAS section for drg table values'''
    lines = sas_section.split('\n')
    values = []
    for line in lines:
        if ' = ' in line:
            drg, value = line.split(' = ')
            vals = (drg.strip().replace("'", ''),
                    value.strip().replace("'", '').strip())
            values .append(vals)
    return values


def parse_hcup_sas(sas_string, format_name):
    '''Simple function to parse the HCUP SAS file for the desired table.'''
    section = get_section(sas_string, format_name)
    values = get_values(section)
    return values


def sas_file_to_dataframe(sas_string, format_name, columns, drgver):
    '''Read the SAS string and parse the desired table and return a
    pandas DataFrame.'''
    values = parse_hcup_sas(sas_string, format_name)
    dataframe = DataFrame(data=values, columns=columns)
    dataframe['drgver'] = drgver
    out_order = ['drgver'] + columns
    return dataframe[out_order]
