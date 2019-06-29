import re


def get_section(sas_string, format_name):
    new_pattern = format_name.replace('*', '\*') + '.*\s+[;]'
    search = re.search(new_pattern, sas_string, re.DOTALL)
    if search:
        return search.string
    else:
        return None
