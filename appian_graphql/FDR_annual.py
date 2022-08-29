import json
import sys
from collections import OrderedDict

from appian_graphql.FDR_handle import FDRHandle
from sql.helpers import extract_and_cleans_fdr_input_from_cmd_line
from sql.helpers import helios_output_data
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


def data_download_with_details_print(
        self,
        data: list,
        params: dict
) -> list:
    """
    Excel spreadsheet requires data in a specific shape and form, we are developing it here
    :param self:
    :param data:
    :param params:
    :return:
    """
    # unique KEYs
    UNQ_TUPLES = AppianFDRConfig.get_unique_combiantion_of_tuples_with_keys(
        data, params['nickname_ids'], params['template_id'], params['sector_id']
    )

    res_list = []
    for nickname_id, agent_id, template_id, sector_id, scale_desc, currency_code,\
        period_type, statement_type, private_flag, fiscal_end_year, exchange_rate in UNQ_TUPLES:
        # unique FDR_IDs
        UNQ_FDR_IDS = sorted(
            list(
                set(
                    [
                        x['fdr_id'] for x in data
                        if x['template_id'] == template_id and x['sector_id']== sector_id
                    ]
                )
            )
        )
        for fdr_id in UNQ_FDR_IDS:
            found_data = False
            res_dict = OrderedDict.fromkeys(
                self._config.get_output_columns_with_details()
            )
            res_dict['NICKNAME_ID'] = nickname_id
            res_dict['AGENT_ID'] = agent_id
            res_dict['TEMPLATE_ID'] = template_id
            res_dict['SECTOR_ID'] = sector_id
            res_dict['FDR_ID'] = fdr_id
            res_dict['SECTOR_NAME'] = (
                self._FDR_MAP.get_sector_name(template_id, sector_id)
            )
            res_dict['TEMPLATE_NAME'] = (
                self._FDR_MAP.get_template_name(template_id, sector_id)
            )
            res_dict['FDR_SECTION'] = (
                self._FDR_MAP.get_fdrid_type(
                    template_id, sector_id, fdr_id
                )
            )
            res_dict['SCALE_DESC'] = scale_desc
            res_dict['CURRENCY_CODE'] = currency_code
            res_dict['STATEMENT_TYPE'] = statement_type
            res_dict['PRIVATE_FLAG'] = private_flag
            res_dict['FISCAL_END_YEAR'] = fiscal_end_year
            res_dict['EXCHANGE_RATE'] = exchange_rate

            # now that we have our keys (i.e. nickname, fdr id) search for datapoints
            # that matches each year
            for year in self._config.get_year_range():
                try:
                    res_dict[str(year)] = [
                        xx for xx in data if
                        xx['nickname_id'] == nickname_id and
                        xx['agent_id'] == agent_id and
                        xx['template_id'] == template_id and
                        xx['sector_id'] == sector_id and
                        xx['fdr_id'] ==  fdr_id  and
                        xx['scale_desc'] == scale_desc and
                        xx['currency_code'] == currency_code and
                        xx['fiscal_year_end'] == fiscal_end_year and
                        xx['exchange_rate'] == exchange_rate and
                        xx['period_type'] == period_type and
                        xx['statement_type'] == statement_type and
                        xx['report_date'] == year


                    ][0]['adjustedValue']
                    # make a note that at least one datapoint for a given year has been found
                    found_data = True
                except IndexError as e:
                    # if not found just keep going
                    pass
            if found_data:
                res_list.append(res_dict)
    return res_list


def data_download_print(
        self,
        data: list,
        params: dict
) -> list:
    # unique FDR_IDs
    UNQ_FDR_IDS = sorted(
        list(
            set(
                [
                    x['fdr_id'] for x in data
                ]
            )
        )
    )

    # Unique Keys
    UNQ_NICKNAMES_AGENTS_TEMPALTES_SECTORS = sorted(
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
    for nickname_id, agent_id, template_id, sector_id in UNQ_NICKNAMES_AGENTS_TEMPALTES_SECTORS:
        for fdr_id in UNQ_FDR_IDS:
            found_data = False
            res_dict = OrderedDict.fromkeys(
                self._config.get_output_columns()
            )
            res_dict['NICKNAME_ID'] = nickname_id
            res_dict['AGENT_ID'] = agent_id
            res_dict['TEMPLATE_ID'] = template_id
            res_dict['SECTOR_ID'] = sector_id
            res_dict['FDR_ID'] = fdr_id
            res_dict['SECTOR_NAME'] = self._FDR_MAP.get_sector_name(template_id, sector_id)
            res_dict['TEMPLATE_NAME'] = self._FDR_MAP.get_template_name(template_id, sector_id)

            # now that we have our keys(i.e. nickname, fdr id) search for datapoints matching each year
            for year in self._config.get_year_range():
                try:
                    res_dict[str(year)] = [
                        xx for xx in data if
                        xx['nickname_id'] == nickname_id and
                        xx['agent_id'] == agent_id and
                        xx['template_id'] == template_id and
                        xx['sector_id'] == sector_id and
                        xx['fdr_id'] == fdr_id and
                        xx['report_date'] == year
                    ][0]['adjustedValue']
                    # make a not that as least one datapoint for a given year has been found
                    found_data = True
                except IndexError as e:
                    # if not found just keep going
                    pass
            # record dictionaries if we have at least one datapoint found
            if found_data:
                res_list.append(res_dict)
    return res_list


try:
    input_header_dict = json.loads(sys.argv[2])
except json.decoder.JSONDecodeError:
    # maybe local test
    input_header_dict = json.loads(
        sys.argv[2].
        replace('"','"').
        replace('"{',"{").
        replace('}"','}')
    )

param_dict = extract_and_cleans_fdr_input_from_cmd_line(input_header_dict)
a = FDRAnnual(param_dict)
a.get_data(a.pivot_function, a.unpivot_function)
a.print_data()





