from abc import ABC, abstractmethod
import requests
import concurrent.futures
import threading
from helpers import get_graph_ql_web_link


class APIInputParams:
    def __init__(
            self,
            url: str,
            request_type: str,
            data: dict = None,
            params: dict = None,
            headers: dict = None,
            pivot_function_params: dict = None,
            unpivot_function_params: dict = None,
    ) -> None:
        """
            create a parameter objects that holds various parameters sent to APIs
            :param url:
            :param data:
            :param params:
            :param headers:
            :param pivot_function_params:
            :param unpivot_function_params:
            """
        self.url = url
        self.request_type = request_type
        self.data = data
        self.params = params
        self.pivot_function_params = pivot_function_params
        self.unpivot_function_params = unpivot_function_params
        self.headers = headers

NO_OF_WORKERS = 20

class APIDataParser:
        def __init__(self):
            self._APPIAN_DATA = []
            self._lock = threading.lock()

        def api_request(
                self,
                url: str,
                request_type: str,
                params: dict = None,
                data: dict = None,
                headers: dict = None,
                pivot_function=None,
                pivot_function_params: dict = None,
                unpivot_function=None,
                unpivot_function_params: dict = None,
                thread_id: int = None):
            """
            sends requests to APIs
            :param url: rul
            :param request_type: post or get
            :param params: parameters passed on to the request call
            :param data: data passed on to the request call
            :param headers: headers
            :param pivot_function: function to prepare a columnar data format
            :param pivot_function_params: parameters used in the pivot function
            :param unpivot_function: function to prepare a print like data format
            :param unpivot_function_params: parameters used in the unpivot_function function
            :param thread_id: can be passed for diagnostics
            :return:
            """
            if request_type == 'get':
                response_data = self.api_get_request(
                    url, params, data, headers
                )
            elif request_type == 'post':
                response_data = self.api_post_request(
                    url, params, data, headers
                )
            self.process_response(
                response_data,
                pivot_function,
                pivot_function_params,
                unpivot_function,
                unpivot_function_params
            )

        def api_get_request(
            self,
            url:str,
            params:dict=None,
            data:dict=None,
            headers:dict=None,
        ) -> dict:
            """
            get request
            :param url: url
            :param params: get parameters
            :param data: get data
            :param headers: headers
            :return: dict
            """
            response = requests.get(
                url=url if url is not None else None,
                params=params if params is not None else None,
                data=data if data is not None else None,
                headers=headers if headers is not None else None,
                )
            response.raise_for_status()
            return response.json()

        def api_post_request(
                self,
                url:str,
                params:dict=None,
                data:dict=None,
                headers:dict=None,
                ):
            """
            get data from Appian API returning JSON
            :param url: url
            :param params: graphql statement
            :param data: returned data
            :param headers: headers
            :return: dict
            """
            response = requests.post(
                url=url if url is not None else None,
                params=params if params is not None else None,
                data=data if data is not None else None,
                headers=headers if headers is not None else None,
            )
            response.raise_for_status()
            return response.json()

        def process_response(
                self,
                response_json:dict,
                pivot_function=None,
                pivot_params=None,
                unpivot_function=None,
                unpivot_params=None
                ):
            """
            unwraps the data and applies pivot and unpivot functions
            :param response_json: returned data
            :param pivot_function: turns data into columnar form
            :param pivot_params: parameters sent to pivot function
            :param unpivot_function: turns columnar data into print like form
            :param unpivot_params: parameters sent to unpivot function
            :return:
            """

            pivot_data = (
                response_json if pivot_function is None
                else pivot_function(response_json, pivot_params)
            )

            unpivot_data = (
                pivot_data if unpivot_function is None
                else unpivot_function(pivot_data, unpivot_params)
            )
            with self._lock:
                if isinstance(unpivot_data, dict):
                    self._APPIAN_DATA.append(unpivot_data)
                else:
                    self._APPIAN_DATA = self._APPIAN_DATA+unpivot_data


class APIEndPint(ABC):

    @abstractmethod
    def build_graph_ql_query_list(self) -> list:
        """
        this procedure should return a list of statements that will be used by thread workers
        to query the APPIAN graph ql API
        :return:
        """
        pass

    @abstractmethod
    def get_data(self):
        """
        note that we have get_appian_data proc below. Therefore, the reason why we need this procedure
        is to force the developers to do the following:
        1. develop build_graph_ql_query_list and provide
        2. build a list of queries
        3. develop pivot_function that can return None
        4. develop unpivot function that can return None
        5. pass 2, 3, 4 from the list into get_appian_data
        """
        pass

    def get_appian_data(
            self,
            list_with_api_objects:list,
            pivot_function:None,
            unpivot_function:None,
            )->list:
        """
        pulls data from graph ql API call
        :param list_with_api_objects: contains api objects that contain input API parameters
        :param pivot_function: turns JSON response into a list of data points
        :param unpivot_function: turns the list of data points into how business expects to receive
        their data
        :param unpivot_function: procedure
        """

        appian_data = APIDataParser()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=NO_OF_WORKERS
        ) as executor:
            for idx, api_obj in enumerate(list_with_api_objects):
                if not isinstance(api_obj, APIInputParams):
                    raise ValueError("list needs to contain valid APIInputParams objects")
                res = executor.submit(
                    lambda p: appian_data.api_request(*p),
                    [
                        api_obj.url,
                        api_obj.request_type,
                        api_obj.params,
                        api_obj.data,
                        api_obj.headers,
                        pivot_function,
                        api_obj.unpivot_function_params,
                        idx
                    ]
                )
                if res.exception() is not None:
                    raise res.exception()
        return appian_data._APPIAN_DATA

    

