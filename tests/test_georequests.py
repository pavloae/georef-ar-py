import unittest

from src.georef_ar_py import georequests
from src.georef_ar_py.check import compare_data
from src.georef_ar_py.georequests import API_BASE_URL


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


class CheckTestCase(unittest.TestCase):

    def test_check_states(self):
        compare_data(API_BASE_URL, "http://52.23.185.155/georef/api/")


if __name__ == '__main__':
    unittest.main()
