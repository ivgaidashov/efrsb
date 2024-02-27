import re
from datetime import datetime

def remove_extra_symbols(value):
    return re.sub(' +', ' ', value.strip().replace('&lt;br /&gt;', '').replace('&lt;br &gt;', '').replace('<br >', '').replace('<br />', '').replace('\t', '').replace('\n', '')[:3999])

def find_node_and_get_value(node, main_tag, child_tag):
    results = node.findall('.//'+main_tag)
    if results:
        for result in results:
            for child in result:
                if child.tag == child_tag:
                    return child.text
    else:
        return 'Not found'


def find_node_and_accumulate(node, main_tag, child_tag, my_type):
    results = node.findall('.//'+main_tag)
    accumulated_float_data = 0.00
    accumulated_str_data = ''
    if results:
        for obligations in results:
            for obligation in obligations:
                if obligation.text and obligation.tag == child_tag:
                    if my_type == 'float':
                        accumulated_float_data += round(float(obligation.text), 2)
                    elif my_type == 'string':
                        accumulated_str_data += obligation.text + ', '

    if my_type == 'string':
        accumulated_str_data = accumulated_str_data[:-2]
        return accumulated_str_data
    elif my_type == 'float':
        return accumulated_float_data


def get_value(value, my_type):
    if value:
        if my_type == 'date':
            return datetime.strptime(value, '%Y-%m-%d')
        elif my_type == 'datestamp':
            if isinstance(value, str):
                return datetime.strptime(value, '%d.%m.%Y %H:%M:%S')
            if isinstance(value, datetime):
                return value
        elif my_type == 'int':
            return int(value)
        elif my_type == 'upper string':
            return str(value).upper()
        elif my_type == 'roubles':
            return "Р{:,.2f}".format(round(float(value), 2))
        else:
            if value:
                return value.strip()
            else:
                return ''
    else:
        return None
