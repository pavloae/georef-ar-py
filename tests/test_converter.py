import os
from unittest import TestCase

from src.georef_ar_py.utils import ndjson2geojson


class TestNdjsonGeojson(TestCase):

    def test_provincias(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/provincias.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/provincias.geojson"))
        )

    def test_departamentos(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/departamentos.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/departamentos.geojson"))
        )

    def test_municipios(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/municipios.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/municipios.geojson"))
        )
        
    def test_localidades_censales(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/localidades_censales.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/localidades_censales.geojson"))
        )
        
    def test_asentamientos(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/asentamientos.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/asentamientos.geojson"))
        )
        
    def test_localidades(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/localidades.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/localidades.geojson"))
        )
        
    def test_calles(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/calles.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/calles.geojson"))
        )
        
    def test_cuadras(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/cuadras.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/cuadras.geojson"))
        )
        
    def test_intersecciones(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ndjson2geojson(
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/intersecciones.ndjson")),
            os.path.join(current_dir, os.path.join(os.getcwd(), "tests/convert/intersecciones.geojson"))
        )
