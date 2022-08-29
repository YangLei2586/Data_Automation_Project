from socket import gethostname
from time import sleep, process_time
import csv
import json
import sys
import os
import requests
from random import random
from collections import namedtuple

_GRAP_QL_PERIOD_PRIORITY = {
    '3 Months - 1st Quarter' : 1,
    '6 Months - Interim': 2,
    '9 Months - 3rd Quarter': 3,
    '12 Months - 4th Quarter': 4,
    'Year End':5
}

_HTTP_MAP = namedtuple(
    'HTTPArg',
    'http_arg_key dict_key_is_required'
)

_HELIOS_NICKNAME_KEY ='dwbi-nickname_list'
_HELIOS_FDR_KEY = 'dwbi-fdr_id_list'
_HELIOS_STATEMENT_MASTER_ID = 'dwbi-statement_id_list'
_HELIOS_USERNAME_KEY = 'dwbi-username'
_HELIOS_END_DATE_KEY = 'dwbi-end_date_str'
_HELIOS_STATEMENT_TYPE_KEY = 'dwbi-statement_type'
_HELIOS_NICKNAME_TEMPLATE_SECTOR_TUPLE_KEY = 'dwbi-nickname_template_sector_tuples'
_HELIOS_TUPLE_SEPARATOR = '|'
_HELIOS_DATA_SEPARATOR = '#'
_HELIOS_DATA_LINE_SEPARATOR = '"'

# navigator fields
_HELIOS_MAX_NUM_TO_RETRIVE = 'dwbi-max_documents_number'
_HELIOS_AGENTS_KEY = 'dwbi-agents'
_HELIOS_OBJECT_IDS = 'dwbi-object_ids'


def get_local_environment_type() -> str:
    """
    check if the script is run locally or from a Dev's laptop
    :return: str
    """
    hostname = gethostname()
    if hostname in ('data-pnp-due-001','dat-pnp-que-001','dat-pnp-pue-001'):
        return 'helios'
    else:
        return 'local'


def list_to_string(
        input_list: list,
        data_separator: str
        ) -> str:
    """
        turns list of numbers into string format if input_list is a string we transform it by
        splitting it at each comma using split function; used predominantly to paste a portion of multiple
        agent IDs into a graph template
        :param input_list: list of int IDs or a string that can be split at each comma
        """
    if isinstance(input_list,str):
        data_list = input_list.split(',')
    else:
        data_list = input_list
    if len(data_list) == 1:
        return f'{data_list[0]}'
    else:
        return data_separator.join(str(__) for __ in data_list)


def helios_output_data(input_list: list):
    """
    """
    if not isinstance(input_list,list):
        raise ValueError(
            "Please provide list"
        )
    # for a list of list separate lines accordingly
    if len(input_list) == 0:
        return ''
    res_string = ''
    if isinstance(input_list[0], list):
        for idx, data_list in enumerate(input_list):
            res_string = res_string+(
                list_to_string(
                    data_list,
                    _HELIOS_TUPLE_SEPARATOR
                )
            )
            if idx < len(input_list)-1:
                res_string = res_string + _HELIOS_DATA_SEPARATOR
            return res_string
    else:
        return list_to_string(
            input_list,
            _HELIOS_TUPLE_SEPARATOR
        )


def get_graph_ql_web_link():
    """
    returns valid graphql link given the environment
    :return:
    """
    _D_ENV = "https://appian-dataservices-api-dev.varimagic.com"
    _Q_ENV = "https://appian-dataservices-api-qa.varimagic.com"
    _P_ENV = "https://appian-dataservices-api.varimagic.com"
    _APP = "data-service/graphql"

    local_env = get_local_environment_type()
    if local_env != 'helios':
        return _P_ENV+_APP

    hostname = gethostname()
    if 'due' in hostname:
        return _D_ENV+_APP
    elif 'que' in hostname:
        return _Q_ENV+ _APP
    else:
        return _P_ENV+_APP


def get_mapping_file_location():
    """
    returns valid graphql link given the environment
    :return:
    """
    _D_ENV = "//abc/dept_shares$/DWBI_DEV/sql/nbfi_fbt/config/template_mapping.json"
    _Q_ENV = "//abc/dept_shares$/DWBI_QA/sql/nbfi_fbt/config/template_mapping.json"
    _P_ENV = "//abc/dept_shares$/DWBI/sql/nbfi_fbt/config/template_mapping.json"

    local_env = get_local_environment_type()
    if local_env != 'helios':
        return _P_ENV

    hostname = gethostname()
    if 'due' in hostname:
        return _D_ENV
    elif 'que' in hostname:
        return _Q_ENV
    else:
        return _P_ENV


def get_fast_api_scheduler_web_link():
    """
    returns valid FAST API scheduler link given the environment
    :return:
    """
    _D_ENV = "https://atlas-dev.varimagic.com"
    _Q_ENV = "https://atlas-qa.varimagic.com"
    _P_ENV = "https://atlas.varimagic.com"
    _APP = "async/models/fast-sl-api/3.0.0"

    local_env = get_local_environment_type()
    if local_env != 'helios':
        return _Q_ENV + _APP

    hostname = gethostname()
    if 'due' in hostname:
        return _D_ENV+_APP
    elif 'que' in hostname:
        return _Q_ENV+ _APP
    else:
        return _P_ENV+_APP


def get_navigator_api_web_link():
    """
    returns valid FAST API scheduler link given the environment
    :return:
    """
    _D_ENV = "https://officedwservice-dev-aws.varimagic.com/api/RatingsNavigator/Search"
    _Q_ENV = "https://officedwservice-qa-aws.varimagic.com/api/RatingsNavigator/Search"
    _P_ENV = "https://officedwservice-aws.varimagic.com/api/RatingsNavigator/Search"

    local_env = get_local_environment_type()
    if local_env != 'helios':
        return _P_ENV

    hostname = gethostname()
    if 'due' in hostname:
        return _D_ENV
    elif 'que' in hostname:
        return _Q_ENV
    else:
        return _P_ENV
