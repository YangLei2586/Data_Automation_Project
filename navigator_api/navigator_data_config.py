import sys
import json
from sql.helpers import _HELIOS_MAX_NUM_TO_RETRIVE, _HELIOS_AGENTS_KEY
from sql.helpers import _HELIOS_OBJECT_IDS
from sql.helpers import get_graph_ql_web_link
from api_integration.api_interface import APIInputParams
from api_integration.api_interface import APIEndPint
from collections import OrderedDict
from operator import itemgetter

_HEADERS = {
    
}