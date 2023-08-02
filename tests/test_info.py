import unittest
from unittest import TestCase, mock

import tests.test_diff
from src.georef_ar_py import info
from src.georef_ar_py.georequests import API_BASE_URL


async def get_mocked_response_async(session, url, entity, **kwargs):
    return tests.test_diff.get_mocked_response(url, entity, **kwargs)


def get_mocked_response(url, entity, **kwargs):
    return tests.test_diff.get_mocked_response(url, entity, **kwargs)


class Test(unittest.IsolatedAsyncioTestCase):

    @mock.patch('src.georef_ar_py.info.get_json')
    @mock.patch('src.georef_ar_py.info.get_json_async')
    def test_get_entity_number(self, get_response_async_mock, get_response_mock):

        get_response_async_mock.side_effect = get_mocked_response_async
        get_response_mock.side_effect = get_mocked_response

        self.assertEqual(24, info.get_entity_number(API_BASE_URL, 'provincias'))

        self.assertEqual(529, info.get_entity_number(API_BASE_URL, 'departamentos'))

        self.assertEqual(2075, info.get_entity_number(API_BASE_URL, 'municipios'))

        self.assertEqual(4027, info.get_entity_number(API_BASE_URL, 'localidades-censales'))

        self.assertEqual(14673, info.get_entity_number(API_BASE_URL, 'asentamientos'))

        self.assertEqual(4037, info.get_entity_number(API_BASE_URL, 'localidades'))

        self.assertEqual(150054, info.get_entity_number(API_BASE_URL, 'calles'))

    @mock.patch('src.georef_ar_py.info.get_json')
    @mock.patch('src.georef_ar_py.info.get_json_async')
    async def test_get_resume(self, get_response_async_mock, get_response_mock):

        get_response_async_mock.side_effect = get_mocked_response_async
        get_response_mock.side_effect = get_mocked_response

        resume = await info.get_resume(API_BASE_URL)

        self.assertTrue(all([key in resume.keys() for key in ['provincias', 'departamentos', 'municipios']]))
