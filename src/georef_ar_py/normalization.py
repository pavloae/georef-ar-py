import asyncio
import logging
from typing import List

import aiohttp
import numpy as np
import tqdm.asyncio as tqdma
from aiohttp import ClientResponseError, ServerDisconnectedError
from requests import HTTPError

from .georequests import API_BASE_URL, get_json_post_async, get_json_async
import pandas as pd

from .utils import flatten_dict

log = logging.getLogger(__name__)

SOURCE_TEMPLATE = {
    'direccion': str,
    'provincia': str,
    'departamento': str,
    'localidad_censal': str,
    'localidad': str
}
NORM_TEMPLATE = {
    "altura": {
                "unidad": str,
                "valor": int
            },
    "calle": {
        "categoria": str,
        "id": str,
        "nombre": str
    },
    "calle_cruce_1": {
        "categoria": str,
        "id": str,
        "nombre": str
    },
    "calle_cruce_2": {
        "categoria": str,
        "id": str,
        "nombre": str
    },
    "departamento": {
        "id": str,
        "nombre": str
    },
    "localidad_censal": {
        "id": str,
        "nombre": str
    },
    "nomenclatura": str,
    "piso": str,
    "provincia": {
        "id": str,
        "nombre": str
    },
    "ubicacion": {
        "lat": float,
        "lon": float
    }
}
ERROR_TEMPLATE = {
    'error': {
        'status_code': str,
        'reason': str
    }
}


class Address:

    def __init__(self, direccion=None, provincia=None, departamento=None, localidad_censal=None, localidad=None) -> None:
        super().__init__()
        self.address = direccion
        self.province = provincia
        self.department = departamento
        self.census_locality = localidad_censal
        self.locality = localidad
        self._normalization = None
        self._error = None

    def __str__(self) -> str:
        return "Direccion: {} - Provincia: {} - Departamento: {} - Localidad censal: {} - Localidad: {} [{}] ".format(
            self.address, self.province, self.department, self.census_locality, self.locality,
            self._normalization.get("nomenclatura", None)
        )

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
        params.update({'campos': 'basico'})
        params.update({'max': 1})
        return params

    @property
    def normalization(self):
        return self._normalization

    @property
    def error(self):
        return self._error

    @normalization.setter
    def normalization(self, normalization):
        self._normalization = normalization

    @error.setter
    def error(self, error):
        self._error = error

    @property
    def data_dict(self):
        if self.normalization:
            return self.normalization
        else:
            return self.error


class AddressNormalizer:

    def __init__(self, url=API_BASE_URL, endpoint="direcciones", chunk_size=1000, data_size=500, rps=None) -> None:
        super().__init__()
        self._addresses = []
        self._url = url
        self._enpoint = endpoint
        self._chunk_size = chunk_size
        self._data_size = data_size
        self._requests_remaining = 0
        self._df = None
        self._pbar = None
        self._normalized_count = 0
        self._errors_count = 0
        self._columns_response = None
        self._rate_limits = None
        self._rps = rps

    @staticmethod
    def row_count(file):
        with open(file, mode="r") as f:
            return sum(1 for _ in f)

    @property
    def columns(self):
        if not self._columns_response:
            self._columns_response = list(
                flatten_dict(NORM_TEMPLATE).keys()
            )
            error_list = list(
                flatten_dict(ERROR_TEMPLATE).keys()
            )
            self._columns_response.extend(error_list)
        return self._columns_response

    @property
    def rate_limits(self):
        if not self._rate_limits:
            pass
        return self._rate_limits

    def _update_address_count(self):
        self._normalized_count += 1
        if self._pbar:
            self._pbar.update(1)

    async def _request(self, session, data=None, **kwargs):
        while self._requests_remaining > 40:
            await asyncio.sleep(1)
        self._requests_remaining += 1
        try:
            if data:
                response = await get_json_post_async(session, self._url, self._enpoint, data, **kwargs)
            else:
                response = await get_json_async(session, self._url, self._enpoint, **kwargs)
            return response
        finally:
            self._requests_remaining -= 1

    async def normalize_address(self, session, address: Address) -> Address:
        """ Consulta a la API para normalizar una dirección

        :param session: Client session for request
        :param address: Un objeto Address con la dirección a normalizar.
        :return: El mismo objeto Address con la dirección normalizada o el error encontrado.
        """

        try:
            response = await self._request(session, **address.get_params_query())
            normalized_addresses = response.get('direcciones', [])
            address.normalization = {} if len(normalized_addresses) == 0 else normalized_addresses[0]
        except HTTPError as re:
            address.error = {
                'error': {
                    'status_code': re.response.status_code,
                    'reason': re.response.reason
                }
            }
        except ClientResponseError as cre:
            address.error = {
                'error': {
                    'status_code': cre.status,
                    'reason': cre.message
                }
            }
        except ServerDisconnectedError as sde:
            address.error = {
                'error': {
                    'status_code': 'Unknown',
                    'reason': sde.message
                }
            }
        except asyncio.TimeoutError:
            address.error = {
                'error': {
                    'status_code': 'Unknown',
                    'reason': "Timeout error"
                }
            }
        except Exception as e:
            address.error = {
                'error': {
                    'status_code': 'Unknown',
                    'reason': "Unknown"
                }
            }
        finally:
            self._update_address_count()
            return address

    async def normalize_batch_addresses(self, session, addresses: List[Address]) -> List[Address]:
        """ Consulta a la API para normalizar un conjunto de direcciones. Si ocurre un error se intentará
        normalizar las direcciones de a una. Esta consulta considera que la API siempre devuelve un listado de
        direcciones normalizadas en el mismo orden en que se envían.

        :param session: Client session for request
        :param addresses: Una lista de objetos Address con direcciones a normalizar.
        :return: La misma lista de objetos Address con las direcciones normalizadas o los errores encontrados.
        """

        try:
            data = {"direcciones": [address.get_params_query() for address in addresses]}
            response = await self._request(session, data=data)
            for address, result in zip(addresses, response.get('resultados')):
                address.normalization = {} if len(result.get('direcciones', [])) == 0 else result.get('direcciones')[0]
                self._update_address_count()
        except Exception:
            await asyncio.gather(*[self.normalize_address(session, address) for address in addresses])
        finally:
            return addresses

    async def _run_normalization(self):
        address_chunk_list = [
            self._addresses[i:i + self._data_size] for i in range(
                0, len(self._addresses), self._data_size
            )
        ]
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                self.normalize_batch_addresses(session, address_chunk) for address_chunk in address_chunk_list
            ])

    def _addresses_to_df(self):
        address_data_list = [address.data_dict for address in self._addresses]
        flatten_list = [flatten_dict(data) for data in address_data_list]
        response_df = pd.DataFrame(flatten_list, columns=self.columns)
        response_df.index = self._df.index
        df_concat = pd.concat([self._df, response_df], axis=1)
        return df_concat

    def _df_to_addresses(self, df_chunk):
        df = df_chunk.filter(items=list(SOURCE_TEMPLATE.keys())).astype('str', errors='ignore')
        df = df.reindex(columns=list(SOURCE_TEMPLATE.keys()))
        df.replace(np.nan, None, inplace=True)

        self._df = df

        self._addresses = []
        for params in self._df.to_dict('records'):
            self._addresses.append(Address(**params))

    def _run_csv_chunk(self, chunk):
        self._df_to_addresses(chunk)
        asyncio.run(self._run_normalization())

    def csv2csv(self, source, target):
        total_count = AddressNormalizer.row_count(source) - 1
        self._normalized_count = 0
        self._errors_count = 0
        with tqdma.tqdm(total=total_count, desc="Direcciones procesadas: ") as bar:
            self._pbar = bar
            self._pbar.total = total_count
            with pd.read_csv(
                    source, keep_default_na=False, chunksize=self._chunk_size, iterator=True
            ) as reader:
                chunk = next(reader)
                self._run_csv_chunk(chunk)
                df = self._addresses_to_df()
                df.to_csv(target, index=False, header=True)
                for chunk in reader:
                    self._run_csv_chunk(chunk)
                    df = self._addresses_to_df()
                    df.to_csv(target, mode='a', index=False, header=False)
        log.info("Direcciones normalizadas: {}".format(self._normalized_count))
        log.info("Errores: {}".format(self._errors_count))

    def normalize(self, address: Address):
        async def func(a):
            async with aiohttp.ClientSession() as session:
                await self.normalize_address(session, a)

        asyncio.run(func(address))

        return address
