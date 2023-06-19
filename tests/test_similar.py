import json
import os
import unittest
from unittest import mock

import src.georef_ar_py
from src.georef_ar_py.similar import get_similar_bulk


def get_mocked_similar_san_juan():
    filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.path.join(os.getcwd(), "tests/response_api/similar_san_juan.json")
    )
    with open(filename, 'r') as f:
        return json.loads(f.read())


def get_mocked_similar_bulk():
    filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.path.join(os.getcwd(), "tests/response_api/similar_bulk.json")
    )
    with open(filename, 'r') as f:
        return json.loads(f.read())


class MyTestCase(unittest.TestCase):

    @mock.patch('src.georef_ar_py.similar.get_json')
    def test_get_similar(self, mock_get_json):
        mock_get_json.return_value = get_mocked_similar_san_juan()
        response = src.georef_ar_py.similar.get_similar(None, 'provincias', 'San Juan')
        self.assertEqual(response[0]['id'], '70')
        self.assertEqual(response[0]['nombre'], 'San Juan')

    @mock.patch('src.georef_ar_py.similar.get_json_post')
    def test_similar_bulk(self, mock_get_json):
        mock_get_json.return_value = get_mocked_similar_bulk()
        response = get_similar_bulk(None, "provincias", ["pxa", "sant fe"])
        self.assertEqual(len(response), 2)
        self.assertEqual(len(response[0]), 0)
        self.assertEqual(response[1]['id'], '82')
        self.assertEqual(response[1]['nombre'], 'Santa Fe')
