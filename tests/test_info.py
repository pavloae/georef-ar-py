from unittest import TestCase, mock

import tests.test_diff
from src.georef_ar_py import info
from src.georef_ar_py.georequests import API_BASE_URL


class Test(TestCase):

    @mock.patch('src.georef_ar_py.info.get_json')
    def test_get_entity_number(self, get_response_mock):

        get_response_mock.side_effect = tests.test_diff.get_mocked_response

        self.assertEqual(24, info.get_entity_number(API_BASE_URL, 'provincias'))

        self.assertEqual(529, info.get_entity_number(API_BASE_URL, 'departamentos'))

        self.assertEqual(2075, info.get_entity_number(API_BASE_URL, 'municipios'))

        self.assertEqual(4027, info.get_entity_number(API_BASE_URL, 'localidades-censales'))

        self.assertEqual(14673, info.get_entity_number(API_BASE_URL, 'asentamientos'))

        self.assertEqual(4037, info.get_entity_number(API_BASE_URL, 'localidades'))

        self.assertEqual(150054, info.get_entity_number(API_BASE_URL, 'calles'))

    @mock.patch('src.georef_ar_py.info.get_json')
    def test_get_resume(self, get_response_mock):

        get_response_mock.side_effect = tests.test_diff.get_mocked_response

        resume = info.get_resume(API_BASE_URL)

        self.assertTrue(all([key in resume.keys() for key in ['provincias', 'departamentos', 'municipios']]))
