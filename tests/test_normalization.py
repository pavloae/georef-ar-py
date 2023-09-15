import os.path
import unittest

from src.georef_ar_py.normalization import normalize_address, normalize_address_batch, csv_to_csv


class Test(unittest.TestCase):

    def test_normalize_address(self):

        response = normalize_address('feliz san martin 135', localidad_censal=58049010)

        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response)

    def test_normalize_address_batch(self):

        input_file = os.path.join(os.getcwd(), "csv/carrefour.csv")
        output_file = os.path.join(os.getcwd(), "csv/carrefour_normalized.csv")
