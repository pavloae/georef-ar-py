import json
import os
import unittest
from unittest import mock

from src.georef_ar_py.territorial import get_territorial_units


def get_mocked_territorial():
    filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.path.join(os.getcwd(), "tests/response_api/territorial.json")
    )
    with open(filename, 'r') as f:
        return json.loads(f.read())


class MyTestCase(unittest.TestCase):

    @mock.patch('src.georef_ar_py.territorial.get_json_post')
    def test_get_territorial_units(self, mock_get_json_post):
        mock_get_json_post.return_value = get_mocked_territorial()
        location1, location2 = get_territorial_units(None, [
            {"lat": -32.9477132, "lon": -60.6304658},
            {"lat": -34.6037389, "lon": -58.3815704}
        ])
        self.assertEqual('Santa Fe', location1['provincia_nombre'])
        self.assertEqual('Rosario', location1['departamento_nombre'])

        self.assertEqual(location2['provincia_nombre'], 'Ciudad Autónoma de Buenos Aires')
        self.assertEqual(location2['departamento_nombre'], 'Comuna 1')
