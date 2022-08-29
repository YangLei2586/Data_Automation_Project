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
        """
        Prepares FDR graph ql statement list
        :param json_headers: HTTP headers passed via Helios call
        :param graph_statement: function to get the latest graph statement
        :return:
        """
        graph_ql = graph_statement
        for http_key, stmnt_key in self._config.get_http_arg_map_key_pairs():

            # if http_key has been sent via Helios call
            # replace the stmnt_key with the received data
            if http_key in json_headers:
                graph_ql = graph_ql.replace(
                    stmnt_key, list_to_string(json_headers[http_key])
                )
            # if http_key has not been sent via Helios call
            # check default fall back parameters
            elif self._config.is_stmt_in_fallback_params(stmnt_key):
                graph_ql = graph_ql.replace(
                    stmnt_key,
                    list_to_string(
                        self._config.get_fallback_graph_key(stmnt_key)
                    )
                )

        # nickname and templates
        nick_and_temps, UNIQUE_TEMPLATE_SECTORS = unwrap_nick_temps_and_sectors(
            json_headers[_HELIOS_NICKNAME_TEMPLATE_SECTOR_TUPLE_KEY]
        )
        statement_list = []
        for template_id, sector_id in UNIQUE_TEMPLATE_SECTORS:
            nicknames_matching_template = [
                __[0] for __ in nick_and_temps
                if __[1] == template_id and __[2] == sector_id

            ]
            fdrs = self._FDR_MAP.get_fdr(
                template_id,
                sector_id
            )
            query_statement = (
                graph_ql.replace(
                    _APPIAN_TEMPLATE_NAME_KEY,
                    self._FDR_MAP.get_template_name(template_id, sector_id)
                ).replace(
                    _APPIAN_NICKNAME_LIST_KEY,
                    list_to_string(nicknames_matching_template)
                ).replace(
                    _APPIAN_FDR_ID_LIST_KEY,
                    list_to_string(fdrs)
                )
            )
            pivot_params = {
                'template_id': template_id,
                'sector_id': sector_id

            }
            unpivot_params = {
                **pivot_params,
                **{'nickname_ids': nicknames_matching_template}
            }
            statement_list.append(
                APIInputParams(
                    url=get_graph_ql_web_link(),
                    request_type='get',
                    params={'query': query_statement},
                    pivot_function_params=pivot_params,
                    unpivot_function_params=unpivot_params
                )
            )
        return statement_list

    def pivot_function(self, p_json_data: dict, metadata: dict = None) -> dict:
        """
        Turns Json response into {{},{},{},{}...}
        :param p_json_data: Json response from the API
        :param metadata: dict
        :return:
        """
        for agent_details in p_json_data['data']['getFDRData']:
            agent_id = agent_details['agent']['agent_id']
            nickname_id = agent_details['nicknameId']
            for agent_data in agent_details['statementMaster']:
                report_date = agent_data['statementDate']
                period_type = get_value_from_api_dict(
                    'periodType', 'periodTypeDesc', agent_data
                )
                for datapoints in agent_data['templateStatementInfo']:
                    # pull datapoints that have been reviewed
                    if datapoints['analystReviewed'] == 'Y':
                        for datapoint in datapoints['stmntData']:
                            try:
                                # only get datapoints with adjusted value present
                                if datapoint['adjustedValue'] is not None:
                                    year_int = int(
                                        datetime.strftime(
                                            datetime.strftime(
                                                report_date,
                                                '%Y-%m-%d'
                                            ),
                                            '%Y'
                                        )
                                    )
                                    data = {
                                        'agent_id': agent_id,
                                        'nickname_id': nickname_id,
                                        'sector_id': metadata['sector_id'],
                                        'template_id': metadata['template_id'],
                                        'report_date': year_int,
                                        'period_type': period_type,
                                        'fdr_id': datapoint['fdrId'],
                                        'adjustedVaule': float(
                                            datapoint['adjustedValue']
                                        )
                                    }
                                    self._OFC.add_data(report_date, period_type, data)
                            except Exception as e:
                                print(
                                    "Error for " + str(agent_id) + "" + str(report_date)
                                    + "" + str(nickname_id) + "" + str(datapoint['fdrId'])
                                    + "" + str(datapoint['adjustedValue'])
                                )
                                print(str(e))
        return self._OFC.get_data()

    def pivot_function_with_details(self, p_json_data: dict, metadata: dict = None) -> dict:
        """
        Turns Json response into {{},{},{},{}...}
        :param p_json_data: Json response from the API
        :param metadata: dict
        :return:
        """
        compare_analyst_flag = True
        if metadata['template_id'] == '2':
            compare_analyst_flag = False

        for agent_details in p_json_data['data']['getFDRData']:
            agent_id = agent_details['agent']['agentId']
            nickname_id = agent_details['nicknameId']
            for agent_data in agent_details['statementMaster']:
                fiscal_end_year = agent_data['fiscalYearEnd']
                report_date = agent_data['statementDate']
                exchange_rate = agent_data['exchangeRate']
                scale_desc = get_value_from_api_dict(
                    'scale', 'scaleDesc', agent_data
                )
                currency_code = get_value_from_api_dict(
                    'currency', 'currencyCode', agent_data
                )
                period_type = get_value_from_api_dict(
                    'periodType', 'periodTypeDesc', agent_data
                )
                statement_type = get_value_from_api_dict(
                    'statementType', 'statementTypeDesc', agent_data
                )
                for datapoints in agent_data['templateStatementInfo']:
                    # pull datapoints that have been reviewed
                    if (
                            (not compare_analyst_flag)
                            or
                            (compare_analyst_flag and datapoints['analystReviewed'] == 'Y')
                    ):
                        private_flag = (
                            None if 'privateFlg' not in datapoints
                            else datapoints['privateFlg']
                        )

                        for datapoint in datapoints['stmntData']:
                            try:
                                if datapoint['adjustedValue'] is not None:
                                    year_int = int(
                                        datetime.strftime(
                                            datetime.strftime(
                                                report_date,
                                                '%Y-%m-%d'
                                            ),
                                            '%Y'
                                        )
                                    )
                                data = {
                                    'agent_id': agent_id,
                                    'nickname_id': nickname_id,
                                    'sector_id': metadata['sector_id'],
                                    'template_id': metadata['template_id'],
                                    'report_date': year_int,
                                    'period_type': period_type,
                                    'fdr_id': datapoint['fdrId'],
                                    'adjustedVaule': float(
                                        datapoint['adjustedValue']
                                    )
                                }
                                self._OFC.add_data(report_date, period_type, data)

                            except Exception as e:
                                print(
                                    "Error for " + str(agent_id) + "" + str(report_date)
                                    + "" + str(nickname_id) + "" + str(datapoint['fdrId'])
                                    + "" + str(datapoint['adjustedValue'])
                                )
                                print(str(e))
        return self._OFC.get_data()

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
        self._FDRData = self.get_appian_data(
            self._FDRStatements,
            pivot_function=pivot_function,
            unpivot_function=unpivot_function
        )

    def print_data(
            self,
            mode='default',
            destination=sys.stdout,
            header=False,
            newline='\n'
    ):
        if mode not in ('default', 'csv', 'json', 'raw'):
            return ValueError(
                'Only None(i.e. std.output),csv, json, raw are allowed'
            )
        if mode == 'default':
            if isinstance(destination, str):
                with open(destination, 'w+', newline=newline) as f:
                    dict_writer = csv.DictWriter(
                        f,
                        self._FDRData[0].keys(),
                        delimiter=self._config.get_print_delimiter()
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
                        delimiter=self._config.get_csv_delimiter()
                    )
                    if header:
                        dict_writer.writeheader()
                    dict_writer.writerows(self._FDRData)

        elif mode == 'json':
            if isinstance(destination, str):
                with open(destination, 'w+', newline=newline) as f:
                    f.write(json.dumps(self._FDRData))
            else:
                # std.output
                sys.stdout.write(json.dumps(self._FDRData))

        elif mode == 'raw':
            return self._FDRData
