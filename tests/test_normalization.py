import asyncio
import os.path
import unittest

import aiohttp

from src.georef_ar_py.normalization import get_normalized_address, get_normalize_addresses_batch, csv_to_csv, Address, \
    AddressNormalizer


class Test(unittest.IsolatedAsyncioTestCase):

    async def test_normalize_address(self):

        async with aiohttp.ClientSession() as session:
            response = await get_normalized_address(session, Address('feliz san martin 135', census_locality=58049010))
        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response['nomenclatura'])

        async with aiohttp.ClientSession() as session:
            response = await get_normalized_address(session, Address('Belgrano 600', census_locality="Córdoba"))
        self.assertEqual('BELGRANO 600, Capital, Córdoba', response['nomenclatura'])

    async def test_normalize_address_list(self):
        address1 = Address('feliz san martin 135', census_locality=58049010)
        address2 = Address('Belgrano 600', census_locality="Córdoba")

        async with aiohttp.ClientSession() as session:
            subtasks = []
            for address in [address1, address2]:
                subtasks.append(get_normalized_address(session, address))

            responses = await asyncio.gather(*subtasks)

            self.assertIsNotNone(responses)

    def test_normalize_address_batch(self):

        input_file = os.path.join(os.getcwd(), "csv/carrefour.csv")
        output_file = os.path.join(os.getcwd(), "csv/carrefour_normalized.csv")

        address_normalizer = AddressNormalizer(url="http://52.23.185.155/georef/api/")
        address_normalizer.load_csv(input_file)
        address_normalizer.export_csv(output_file)
        #
        # csv_to_csv(input_csv=input_file, output_csv=output_file, url="http://52.23.185.155/georef/api/", chuck=1000, max=1)
