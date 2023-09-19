import asyncio
import logging
from typing import List, Union

import aiohttp
import numpy as np
from aiohttp import ClientResponseError, ServerDisconnectedError
from requests import HTTPError

from .georequests import API_BASE_URL, get_json_async, get_json_post_async
import pandas as pd

from .utils import flatten_dict

log = logging.getLogger(__name__)


class Address:

    def __init__(self, direccion=None, provincia=None, departamento=None, localidad_censal=None, localidad=None) -> None:
        super().__init__()
        self.address = direccion
        self.province = provincia
        self.department = departamento
        self.census_locality = localidad_censal
        self.locality = localidad
        self._normalization = {}

    def __str__(self) -> str:
        return "Direccion: {} - Provincia: {} - Departamento: {} - Localidad censal: {} - Localidad: {} [{}] ".format(
            self.address, self.province, self.department, self.census_locality, self.locality, self.nomencla
        )

    def validate(self):
        pass
        # if not self.address or self.address == '':
        #     raise ValueError("El campo direccion debe estar presente y no puede estar vacío")

    def get_params_query(self):
        params = {'direccion': self.address}
        if self.province:
            params.update({'provincia': self.province})
        if self.department:
            params.update({'departamento': self.department})
        if self.census_locality:
            params.update({'localidad_censal': self.census_locality})
        if self.locality:
            params.update({'localidad': self.locality})
        return params

    def get_normalization(self):
        return self._normalization

    @property
    def nomencla(self):
        return self._normalization.get("nomenclatura", None)


class AddressNormalizer:

    def __init__(self, url=API_BASE_URL, chunk_size=1000) -> None:
        super().__init__()
        self._addresses = []
        self._normalized = False
        self._url = url
        self._chunk_size = chunk_size
        self._processed = 0

    def load_csv(self, csv_file):
        csv_source = pd.read_csv(csv_file, keep_default_na=False).filter(items=[
            'direccion',
            'localidad_censal',
            'localidad',
            'departamento',
            'provincia'
        ]).astype('str', errors='ignore')
        csv_source.replace(np.nan, None, inplace=True)

        self.reset()
        for params in csv_source.to_dict('records'):
            self._addresses.append(Address(**params))

    def validate(self):
        try:
            for pos, address in enumerate(self._addresses):
                address.validate()
        except ValueError as ve:
            raise ValueError("Error en el registro {}. {}".format(pos + 1, ve.__str__()))

    def reset(self):
        self._addresses = []
        self._normalized = False

    async def run_normalization_chunk(self, session, chunk: List[Address]):
        addresses = [address.get_params_query() for address in chunk]
        try:
            normalized_addresses = await normalize_addresses(session, addresses, self._url)
            for address, normalization in zip(chunk, normalized_addresses):
                address._normalization = normalization
            self._processed += len(chunk)
            print("Procesados... {} de {}".format(self._processed, len(self._addresses)))
            return normalized_addresses
        except Exception as re:
            response = await asyncio.gather(
                *[normalize_address(session, address.address, self._url, **address.get_params_query()) for address in chunk]
            )
            for address, normalization in zip(chunk, response):
                address._normalization = normalization
            self._processed += len(chunk)
            print("Procesados... {} de {}".format(self._processed, len(self._addresses)))
            return response
        except Exception as e:
            raise e

    async def run_normalization(self):
        chunks = [self._addresses[i:i + self._chunk_size] for i in range(0, len(self._addresses), self._chunk_size)]
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[self.run_normalization_chunk(session, chunk) for chunk in chunks])

    @property
    def normalized_addresses(self):
        if not self._normalized:
            asyncio.run(self.run_normalization())
        return [address.get_normalization() for address in self._addresses]

    def export_csv(self, csv_file, prefix=''):
        rows = [flatten_dict(address, prefix=prefix) for address in self.normalized_addresses]
        pd.DataFrame(rows).to_csv(csv_file, index=False, header=True)


async def normalize_address(session, address, url=API_BASE_URL, **kwargs) -> dict:
    """ Consulta a la API para normalizar una dirección

    :param session: Client session for request
    :param address: Una dirección a normalizar en forma de texto.
    :param url: La url de la API a ser consultada
    :param kwargs: Los parámetros de la consulta (optativos) [
    'provincia', 'departamento', 'localidad_censal', 'localidad'
    ], y otros parámetros extra.
    :return: Un diccionario con los datos de la normalización en caso de éxito, un diccionario vacío si no se encontró
    un resultado, o un diccionario con los errores en caso de producirse:

    """
    if address:
        kwargs.update({'direccion': address})
    try:
        response = await get_json_async(session, url, 'direcciones', **kwargs)
        normalized_addresses = response.get('direcciones', [])
        return {} if len(normalized_addresses) == 0 else normalized_addresses[0]
    except HTTPError as re:
        return {
            'error': {
                'status_code': re.response.status_code,
                'reason': re.response.reason
            }
        }
    except ClientResponseError as cre:
        return {
            'error': {
                'status_code': cre.status,
                'reason': cre.message
            }
        }
    except ServerDisconnectedError as sde:
        return {
            'error': {
                'status_code': 'Unknown',
                'reason': sde.message
            }
        }
    except asyncio.TimeoutError as toe:
        return {
            'error': {
                'status_code': 'Unknown',
                'reason': "Timeout error"
            }
        }
    except Exception as e:
        return {
            'error': {
                'status_code': 'Unknown',
                'reason': "Unknown"
            }
        }


async def normalize_addresses(session, addresses: List[dict], url=API_BASE_URL, **kwargs) -> List[dict]:
    """ Consulta a la API para normalizar un conjunto de direcciones

    :param session: Client session for request
    :param addresses: Una lista de diccionarios con los parámetros de consulta para cada dirección.
    :param url: La url de la API a ser consultada
    :param kwargs: Parámetros extra. Los específicos de cada consultan van incorporados en cada diccionario.
    :return:
    """
    data = {"direcciones": addresses}
    response = await get_json_post_async(session, url, "direcciones", data, **kwargs)
    direcciones = []
    for result in response.get('resultados'):
        direcciones.append({} if len(result.get('direcciones', [])) == 0 else result.get('direcciones')[0])
    return direcciones
