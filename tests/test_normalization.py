import os.path
import unittest

from src.georef_ar_py.normalization import get_normalized_address, get_normalize_addresses_batch, csv_to_csv


class Test(unittest.TestCase):

    def test_normalize_address(self):

        response = get_normalized_address('feliz san martin 135', localidad_censal=58049010)

        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response['nomenclatura'])

    def test_normalize_address_batch(self):

        input_file = os.path.join(os.getcwd(), "csv/indec.csv")
        output_file = os.path.join(os.getcwd(), "csv/indec_normalized.csv")

        csv_to_csv(input_csv=input_file, output_csv=output_file, url="http://52.23.185.155/georef/api/", chuck=1000, max=1)
