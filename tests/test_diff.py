import json
import os
import unittest
from unittest import mock

from src.georef_ar_py.diff import get_diff_object, DiffEntity


def get_mocked_response(url, entity, **kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    entity_files = {
        'provincias': os.path.join(os.getcwd(), "tests/response_api/provincias.json"),
        'departamentos': os.path.join(os.getcwd(), "tests/response_api/departamentos.json"),
        'municipios': os.path.join(os.getcwd(), "tests/response_api/municipios.json"),
        'localidades-censales': os.path.join(os.getcwd(), "tests/response_api/localidades-censales.json"),
        'asentamientos': os.path.join(os.getcwd(), "tests/response_api/asentamientos.json"),
        'localidades': os.path.join(os.getcwd(), "tests/response_api/localidades.json"),
        'calles': os.path.join(os.getcwd(), "tests/response_api/calles.json"),
    }
    filename = os.path.join(current_dir, entity_files[entity])
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    return data


def get_mocked_entity_number(url, entity, **kwargs):
    return get_mocked_response(url, entity)['total']


class DiffTestCase(unittest.IsolatedAsyncioTestCase):
    """
        Agrega tests para verificar la cantidad de entidades en cada capa y el correcto funcionamiento de la clase DiffEntity
    """

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

    def setUp(self) -> None:
        super().setUp()
        self.path_dir = os.getcwd()
        self.src_url = "http://52.23.185.155/georef/api/"
        self.target_url = "http://52.23.185.155/georef/api/"

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_response")
    async def test_diff_provinces(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "provincias")

        diff_entity = get_diff_object(self.src_url, self.target_url, 'provincias')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(24, len(src_registers))
        self.assertEqual(24, len(target_registers))

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_response")
    async def test_diff_departments(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "departamentos")

        diff_entity = get_diff_object(self.src_url, self.target_url, 'departamentos')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(len(src_registers), 10)
        self.assertEqual(len(target_registers), 10)

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_response")
    async def test_diff_municipalities(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "municipios")

        diff_entity = get_diff_object(self.src_url, self.target_url, 'municipios')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(len(src_registers), 10)
        self.assertEqual(len(target_registers), 10)

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_response")
    async def test_diff_census_localities(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "localidades-censales")

        diff_entity = get_diff_object(self.src_url, self.target_url, 'localidades-censales')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(len(src_registers), 10)
        self.assertEqual(len(target_registers), 10)

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_registers_by_region")
    async def test_diff_settlements(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "asentamientos")['asentamientos']

        diff_entity = get_diff_object(self.src_url, self.target_url, 'asentamientos')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(len(src_registers), 10)
        self.assertEqual(len(target_registers), 10)

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_response")
    async def test_diff_localities(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "localidades")

        diff_entity = get_diff_object(self.src_url, self.target_url, 'localidades')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(len(src_registers), 10)
        self.assertEqual(len(target_registers), 10)

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)

    @mock.patch('src.georef_ar_py.diff.get_entity_number')
    @mock.patch.object(DiffEntity, "_get_registers_by_region")
    async def test_diff_streets(self, mock_method, mock_entity_number):
        mock_entity_number.side_effect = get_mocked_entity_number
        mock_method.return_value = get_mocked_response(self.target_url, "calles")['calles']

        diff_entity = get_diff_object(self.src_url, self.target_url, 'calles')

        src_registers = await diff_entity.get_src_registers(None)
        target_registers = await diff_entity.get_src_registers(None)

        self.assertEqual(len(src_registers), 10)
        self.assertEqual(len(target_registers), 10)

        diff_dict = await diff_entity._get_diff_as_dict()

        self.assertEqual({}, diff_dict)
