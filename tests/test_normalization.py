import asyncio
import logging
import os.path
import unittest

import aiohttp

from src.georef_ar_py.__main__ import get_logger
from src.georef_ar_py.normalization import normalize_address, normalize_addresses, AddressNormalizer


class Test(unittest.IsolatedAsyncioTestCase):

    async def test_normalize_address(self):

        async with aiohttp.ClientSession() as session:
            response = await normalize_address(session, 'feliz san martin 135', localidad_censal=58049010)
        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response['nomenclatura'])

        async with aiohttp.ClientSession() as session:
            response = await normalize_address(session, 'Belgrano 600', localidad_censal="Córdoba")
        self.assertEqual('BELGRANO 600, Capital, Córdoba', response['nomenclatura'])

    async def test_normalize_address_list(self):
        address1 = {'direccion': 'feliz san martin 135', 'localidad_censal': 58049010}
        address2 = {'direccion': 'Belgrano 600', 'localidad_censal': 'Córdoba'}

        async with aiohttp.ClientSession() as session:
            subtasks = []
            for address in [address1, address2]:
                direccion = address.pop('direccion')
                subtasks.append(normalize_address(session, direccion, **address))

            response = await asyncio.gather(*subtasks)

        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response[0]['nomenclatura'])
        self.assertEqual('BELGRANO 600, Capital, Córdoba', response[1]['nomenclatura'])

    async def test_normalize_addresses(self):
        address1 = {"direccion": "feliz san martin 135", 'localidad_censal': "58049010", "campos": "basico", "max": 1}
        address2 = {"direccion": "Belgrano 600", "campos": "basico", "max": 1, "localidad_censal": "Córdoba"}

        async with aiohttp.ClientSession() as session:
            response = await normalize_addresses(session, [address1, address2])

        self.assertEqual('FELIX SAN MARTIN 135, Huiliches, Neuquén', response[0]['nomenclatura'])
        self.assertEqual('BELGRANO 600, Capital, Córdoba', response[1]['nomenclatura'])

    def test_normalize_address_batch(self):

        get_logger(logging.DEBUG)

        input_file = os.path.join(os.getcwd(), "csv/carrefour.csv")
        output_file = os.path.join(os.getcwd(), "csv/carrefour_normalized.csv")

        address_normalizer = AddressNormalizer(url="http://52.23.185.155/georef/api/", chunk_size=100)
        address_normalizer.load_csv(input_file)
        address_normalizer.validate()
        address_normalizer.export_csv(output_file)
