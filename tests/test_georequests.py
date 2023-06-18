import logging
import unittest
from unittest import TestCase

import pytest as pytest
from requests import RequestException

from src.georef_ar_py import georequests
from src.georef_ar_py.georequests import get_json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MyTestCase(unittest.TestCase):

    def test_get_similar(self):
        response = georequests.get_similar('provincias', 'San Juan')
        self.assertEqual(response[0]['id'], '70')
        self.assertEqual(response[0]['nombre'], 'San Juan')

    def test_similar_bulk(self):
        response = georequests.get_similar_bulk("provincias", ["pxa", "sant fe"])
        self.assertEqual(len(response), 2)
        self.assertEqual(len(response[0]), 0)
        self.assertEqual(response[1]['id'], '82')
        self.assertEqual(response[1]['nombre'], 'Santa Fe')

    def test_get_territorial_units(self):
        location1, location2 = georequests.get_territorial_units([
            {"lat": -32.9477132, "lon": -60.6304658},
            {"lat": -34.6037389, "lon": -58.3815704}
        ])
        self.assertEqual('Santa Fe', location1['provincia_nombre'])
        self.assertEqual('Rosario', location1['departamento_nombre'])

        self.assertEqual(location2['provincia_nombre'], 'Ciudad Autónoma de Buenos Aires')
        self.assertEqual(location2['departamento_nombre'], 'Comuna 1')


class Test(TestCase):
    def test_get_json(self):
        with pytest.raises(RequestException):
            get_json(georequests.API_BASE_URL, 'paises')
