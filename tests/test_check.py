import json
import os
import unittest
from unittest import mock

from src.georef_ar_py.check import get_diff_entity, DiffEntity


def get_mocked_response(entity):
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


class DiffTestCase(unittest.TestCase):
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

    @mock.patch.object(DiffEntity, "_get_response")
    def test_diff_provinces(self, mock_method):
        mock_method.return_value = get_mocked_response("provincias")

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'provincias')

        self.assertEqual(24, len(diff_entity.src_registers))
        self.assertEqual(24, len(diff_entity.target_registers))
        self.assertEqual({}, diff_entity._get_diff_as_dict())

    @mock.patch.object(DiffEntity, "_get_response")
    def test_diff_departments(self, mock_method):
        mock_method.return_value = get_mocked_response("departamentos")

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'departamentos')

        self.assertEqual(len(diff_entity.src_registers), 10)
        self.assertEqual(len(diff_entity.target_registers), 10)
        self.assertEqual({}, diff_entity._get_diff_as_dict())

    @mock.patch.object(DiffEntity, "_get_response")
    def test_diff_municipalities(self, mock_method):
        mock_method.return_value = get_mocked_response("municipios")

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'municipios')

        self.assertEqual(len(diff_entity.src_registers), 10)
        self.assertEqual(len(diff_entity.target_registers), 10)
        self.assertEqual({}, diff_entity._get_diff_as_dict())

    @mock.patch.object(DiffEntity, "_get_response")
    def test_diff_census_localities(self, mock_method):
        mock_method.return_value = get_mocked_response("localidades-censales")

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'localidades-censales')

        self.assertEqual(len(diff_entity.src_registers), 10)
        self.assertEqual(len(diff_entity.target_registers), 10)
        self.assertEqual({}, diff_entity._get_diff_as_dict())

    @mock.patch.object(DiffEntity, "_get_registers_by_region")
    def test_diff_settlements(self, mock_method):
        mock_method.return_value = get_mocked_response("asentamientos")['asentamientos']

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'asentamientos')

        self.assertEqual(len(diff_entity.src_registers), 10)
        self.assertEqual(len(diff_entity.target_registers), 10)
        self.assertEqual({}, diff_entity._get_diff_as_dict())

    @mock.patch.object(DiffEntity, "_get_response")
    def test_diff_localities(self, mock_method):
        mock_method.return_value = get_mocked_response("localidades")

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'localidades')

        self.assertEqual(len(diff_entity.src_registers), 10)
        self.assertEqual(len(diff_entity.target_registers), 10)
        self.assertEqual({}, diff_entity._get_diff_as_dict())

    @mock.patch.object(DiffEntity, "_get_registers_by_region")
    def test_diff_streets(self, mock_method):
        mock_method.return_value = get_mocked_response("calles")['calles']

        diff_entity = get_diff_entity(self.src_url, self.target_url, 'calles')

        self.assertEqual(len(diff_entity.src_registers), 10)
        self.assertEqual(len(diff_entity.target_registers), 10)
        self.assertEqual({}, diff_entity._get_diff_as_dict())


if __name__ == '__main__':
    unittest.main()
