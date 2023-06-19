import logging
from unittest import TestCase

import pytest as pytest
from requests import RequestException

from src.georef_ar_py import georequests
from src.georef_ar_py.georequests import get_json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Test(TestCase):
    def test_get_json(self):
        with pytest.raises(RequestException):
            get_json(georequests.API_BASE_URL, 'paises')
