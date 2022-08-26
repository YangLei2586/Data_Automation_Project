import json
import sys
from collections import OrderedDict

from appian_graphql.FDR_handle import FDRHandle
from helpers import extract_and_cleans_fdr_input_from_cmd_line
from helpers import helios_output_data
from config.appian_fdr_config import AppianFDRConfig


class FDRAnnual(FDRHandle):

    def __init__(
            self,
            cmd_arg_str: str,
            fdr_type: str,
            graph_statement: str
            ):
        super().__init__(
            cmd_arg_str,
            'annual',
            fdr_type,
            graph_statement
        )

    def overview_tab_print(
            self,
            data: list,
            params: list
            ) -> list:
        """
        Excel spreadsheet requires data in a specific shape and form we are building it here
        :param data:
        :param params:
        :return:
        """
        UNQ_NICKNAMES_AGENTS_TEMPLATES_SECTORS = sorted(
            list(
                set(
                    [
                        (
                            x['nickname_id'],
                            x['agent_id'],
                            x['template_id'],
                            x['sector_id']
                        )
                        for x in data
                    ]
                )
            )
        )

        res_list = []
        for nickname_id, agent_id, template_id, sector_id in UNQ_NICKNAMES_AGENTS_TEMPLATES_SECTORS:

            UNQ_YEARS = sorted(
                list(
                    set(
                        [
                            x['report_date'] for x in data
                            if x['agent_id'] == agent_id
                            and x['nickname_id'] == nickname_id
                        ]
                    )
                ),
                reverse = True
            )
            res_dict = OrderedDict()
            fdrs = self._FDR_MAP.get_fdr(template_id, sector_id)
            res_dict['sector_id'] = sector_id
            res_dict['template_id'] = template_id
            res_dict['first_year'] = UNQ_YEARS[0]
            res_dict['years'] = helios_output_data(
                UNQ_YEARS[1:] if len(UNQ_YEARS) > 1 else []
            )

            data_list = []
            for idx, year in enumerate(UNQ_YEARS):
                data_list.append([None for __ in range(len(fdrs))])
                for fdr_idx, fdr in enumerate(fdrs):
                    try:
                        datapoint = [
                            __['adjustedValue']
                            for __ in data
                            if __['agent_id'] == agent_id
                            and __['nickname_id'] == nickname_id
                            and __['report_date'] == year
                            and __['fdr_id'] == fdr
                            and __['template_id'] == template_id
                            and __['sector_id'] == sector_id
                        ]
                        data_list[idx][fdr_idx] = datapoint[0]
                    except Exception:
                        continue

            res_dict['first_data_column'] = helios_output_data(data_list[0])
            res_dict['data'] = helios_output_data(data_list[1:])
            res_dict['agent_id'] = agent_id
            res_list.append(res_dict)
        return res_list
