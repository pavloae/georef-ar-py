from unittest import TestCase, mock

import tests.test_check
from src.georef_ar_py import info
from src.georef_ar_py.georequests import API_BASE_URL


class Test(TestCase):

    @mock.patch('src.georef_ar_py.info._get_response')
    def test_get_entity_number(self, get_response_mock):
        get_response_mock.return_value = tests.test_check.get_mocked_response('provincias')
        self.assertEqual(24, info.get_entity_number(API_BASE_URL, 'provincias'))

        get_response_mock.return_value = tests.test_check.get_mocked_response('departamentos')
        self.assertEqual(529, info.get_entity_number(API_BASE_URL, 'departamentos'))

        get_response_mock.return_value = tests.test_check.get_mocked_response('municipios')
        self.assertEqual(2075, info.get_entity_number(API_BASE_URL, 'municipios'))

        get_response_mock.return_value = tests.test_check.get_mocked_response('localidades-censales')
        self.assertEqual(4027, info.get_entity_number(API_BASE_URL, 'localidades-censales'))

        get_response_mock.return_value = tests.test_check.get_mocked_response('asentamientos')
        self.assertEqual(14673, info.get_entity_number(API_BASE_URL, 'asentamientos'))

        get_response_mock.return_value = tests.test_check.get_mocked_response('localidades')
        self.assertEqual(4037, info.get_entity_number(API_BASE_URL, 'localidades'))

        get_response_mock.return_value = tests.test_check.get_mocked_response('calles')
        self.assertEqual(150054, info.get_entity_number(API_BASE_URL, 'calles'))
