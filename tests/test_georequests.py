import logging
from unittest import TestCase

import pytest as pytest
from requests import RequestException

from georef_ar_py import georequests
from georef_ar_py.georequests import get_json, get_limits

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Test(TestCase):
    def test_get_json(self):
        with pytest.raises(RequestException):
            get_json(georequests.API_BASE_URL, 'paises')

    def test_get_limits(self):
        limits = get_limits(georequests.API_BASE_URL, 'direcciones')
        self.assertIsNotNone(limits)
        self.assertEqual(8, len(limits))

