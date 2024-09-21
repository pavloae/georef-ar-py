import asyncio
import logging
import os.path
import unittest

import aiohttp

from georef_ar_py import utils
from georef_ar_py.__main__ import get_logger
from georef_ar_py.normalization import AddressNormalizer, Address, NORM_TEMPLATE


class Test(unittest.IsolatedAsyncioTestCase):

    def test_normalize_address(self):

        address_normalizer = AddressNormalizer()
        address = Address('feliz san martin 135', localidad_censal=58049010)
        address = address_normalizer.normalize(address)
        self.assertIsNone(address.error)
        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', address.normalization['nomenclatura'])

        address = Address('Belgrano 600', localidad_censal="Córdoba")
        address = address_normalizer.normalize(address)
        self.assertIsNone(address.error)
        self.assertEqual('BELGRANO 600, Capital, Córdoba', address.normalization['nomenclatura'])

    async def test_normalize_address_list(self):
        address1 = Address('feliz san martin 135', localidad_censal=58049010)
        address2 = Address('Belgrano 600', localidad_censal='Córdoba')

        address_normalizer = AddressNormalizer()
        async with aiohttp.ClientSession() as session:
            subtasks = []
            for address in [address1, address2]:
                subtasks.append(address_normalizer.normalize_address(session, address))

            response = await asyncio.gather(*subtasks)

        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response[0].normalization['nomenclatura'])
        self.assertEqual('BELGRANO 600, Capital, Córdoba', response[1].normalization['nomenclatura'])

    async def test_normalize_addresses(self):
        address1 = Address("feliz san martin 135", localidad_censal="58049010")
        address2 = Address("Belgrano 600", localidad_censal="Córdoba")

        address_normalizer = AddressNormalizer()
        async with aiohttp.ClientSession() as session:
            response = await address_normalizer.normalize_batch_addresses(session, [address1, address2])

        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response[0].normalization['nomenclatura'])
        self.assertEqual('BELGRANO 600, Capital, Córdoba', response[1].normalization['nomenclatura'])

    def test_normalize_address_batch(self):

        get_logger(logging.DEBUG)

        input_file = os.path.join(os.getcwd(), "csv/carrefour.csv")
        output_file = os.path.join(os.getcwd(), "csv/carrefour_normalized.csv")

        address_normalizer = AddressNormalizer()
        address_normalizer.csv2csv(input_file, output_file)

    def test_flatten_dict(self):
        output = list(utils.flatten_dict(NORM_TEMPLATE).keys())
        self.assertIsNotNone(output)
