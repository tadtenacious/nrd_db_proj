import re


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
        if '=' in line:
            drg, value = line.split('=')
            vals = (drg.strip().replace("'", ''),
                    value.strip().replace("'", ''))
            values .append(vals)
    return values
