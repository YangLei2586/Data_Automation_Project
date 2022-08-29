import requests
from datetime import datetime
import json
import sys
import csv

from api_integration.api_interface import APIEndPint
from data_structures.ordered_fdr_date import DataOrderedContainer
from config.appian_fdr_config import AppianFDRConfig
from config.appian_fdr_config import _APPIAN_TEMPLATE_NAME_KEY
from config.appian_fdr_config import _APPIAN_NICKNAME_LIST_KEY
from config.appian_fdr_config import _APPIAN_FDR_ID_LIST_KEY
from api_integration.api_interface import APIInputParams
from sql.helpers import list_to_string, unwrap_nick_temps_and_sectors
from sql.helpers import get_graph_ql_web_link, get_value_from_api_dict
from sql.helpers import _HELIOS_NICKNAME_TEMPLATE_SECTOR_TUPLE_KEY
from config.fdr_template_mapping import FDRTemplateMap


class FDRHandle(APIEndPint):

    def __init__(
            self,
            cmd_arg_str: str,
            operation_mode: str,
            fdr_type: str,
            graph_statement: str
    ):
        """
        Builds initial statement given the input dictionary. cmd_arg_str is received as string at first
        from the command line arguments
        :param cmd_arg_str: arguments from the command line dict like structure but string format
        :param operation_mode: annual or latest, FDR data aggregation
        :param fdr_type: core, complimentary or all
        :param graph_statement:
        """
        self._config = AppianFDRConfig()
        self._FDR_MAP = FDRTemplateMap(fdr_type)
        self._FDRStatements = (
            self.build_graph_ql_query_list(
                json.loads(cmd_arg_str),
                graph_statement
            )
        )
        self._OFC = DataOrderedContainer(operation_mode)

    def build_graph_ql_query_list(
            self,
            json_headers: dict,
            graph_statement: str
    ) -> list:

    def pivot_function(self,p_json_data: dict, metadata:dict=None) -> dict:
        """
        Turns Json response into {{},{},{},{}...}
        :param p_json_data: Json response from the API
        :param metadata: dict
        :return:
        """
    def pivot_function_with_details(self, p_json_data: dict, metadata: dict = None)->dict:
        """
        Turns Json response into {{},{},{},{}...}
        :param p_json_data: Json response from the API
        :param metadata: dict
        :return:
        """
    def get_data(
            self,
            pivot_function,
            unpivot_function
    ):
        """

        :param pivot_function:
        :param unpivot_function:
        :return:
        """
    def print_data(
            self,
            mode = 'default',
            destination = sys.stdout,
            header = False,
            newline = '\n'
    ):
        if mode not in ('default','csv','json','raw'):
            return ValueError(
                'Only None(i.e. std.output),csv, json, raw are allowed'
            )
        if mode == 'default':
            if isinstance(destination, str):
                with open(destination, 'w+', newline=newline) as f:
                    dict_writer = csv.DictWriter(
                        f,
                        self._FDRData[0].keys(),
                        delimiter = self._config.get_print_delimiter()
                     )
                    if header:
                        dict_writer = csv.DictWriter()
                    dict_writer.writerows(self._FDRData)
        elif mode == 'csv':
            if isinstance(destination, str):
                with open(destination, 'w+', newline=newline) as f:
                    dict_writer = csv.DictWriter(
                        f,
                        self._FDRData[0].keys(),
                        delimiter = self._config.get_csv_delimiter()
                    )
                    if header:
                        dict_writer.writeheader()
                    dict_writer.writerows(self._FDRData)

        elif mode == 'json':
            if isinstance(destination,str):
                with open(destination, 'w+', newline=newline) as f:
                    f.write(json.dumps(self._FDRData))
            else:
                # std.output
                sys.stdout.write(json.dumps(self._FDRData))

        elif mode == 'raw':
            return self._FDRData
        



