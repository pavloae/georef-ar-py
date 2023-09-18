import logging

from requests import HTTPError

from .exceptions import AddressNormalizationException
from .georequests import API_BASE_URL, get_json, get_json_post, get_json_async
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
        self.normalization = {}

    def __str__(self) -> str:
        return "{}. Provincia: {} - Departamento: {} - Localidad: {}".format(self.address, self.province, self.department, self.census_locality)

    def get_params_query(self):
        params = {}
        if self.province:
            params.update({'provincia': self.province})
        if self.department:
            params.update({'departamento': self.department})
        if self.census_locality:
            params.update({'localidad_censal': self.census_locality})
        if self.locality:
            params.update({'localidad': self.locality})
        return params

    def get_normalized(self):
        return self.normalization

class AddressNormalizer:

    def __init__(self, url=API_BASE_URL, chunk_size=1000) -> None:
        super().__init__()
        self.addresses = []
        self.url = url
        self.chunk_size = chunk_size

    def load_csv(self, csv_file):
        csv_source = pd.read_csv(csv_file).filter(items=[
            'direccion',
            'localidad_censal',
            'localidad',
            'departamento',
            'provincia'
        ]).astype('str', errors='ignore')

        self.addresses = []
        for params in csv_source.to_dict('records'):
            self.addresses.append(Address(**params))

        for address in self.addresses:
            print(address)

    def request_addresses(self):
        pass

    def export_csv(self, csv_file):
        normalized_addresses = [address.get_normalized() for address in self.addresses]
        pd.DataFrame(normalized_addresses).to_csv(csv_file, index=False, header=True)


async def get_normalized_address(session, address: Address, url=API_BASE_URL, **kwargs):
    for key, value in address.get_params_query().items():
        kwargs[key] = value

    try:
        response = await get_json_async(session, url, 'direcciones', direccion=address.address, **kwargs)
        normalized_addresses = response.get('direcciones', [])
        return {} if len(normalized_addresses) == 0 else normalized_addresses[0]
    except HTTPError as re:
        log.error("La consulta falló! status_code: {} - reason: {}".format(re.response.status_code, re.response.reason))
        raise AddressNormalizationException(address, re.response)


async def get_normalize_addresses_batch(session, addresses, url=API_BASE_URL, **kwargs):
    data = {'direcciones': addresses}
    try:
        response = get_json_post(url, 'direcciones', data, **kwargs)
        resultados = response.get('resultados', [])
        direcciones = list(a.get('direcciones') for a in resultados)
        return direcciones
    except AddressNormalizationException as ane:
        normalized_addresses = []
        for address in addresses:
            try:
                normalized_addresses.append(get_normalized_address(session, address, url, **kwargs))
            except AddressNormalizationException as ane:
                normalized_addresses.append({})


def csv_to_csv(input_csv, output_csv, url=API_BASE_URL, chunk=1000, **kwargs):
    df_source = pd.read_csv(input_csv).filter(items=[
        'direccion',
        'localidad_censal',
        'localidad',
        'departamento',
        'provincia'
    ]).astype('str', errors='ignore')
    df_query = df_source.copy()

    params = [
        'orden',
        'aplanar',
        'campos',
        'max',
        'inicio',
        'exacto'
    ]

    for param in filter(lambda param: param in kwargs.keys(), params):
        df_query[param] = kwargs.get(param)

    df_query_list = [df_query[i:i + chunk] for i in range(0, df_query.shape[0], chunk)]

    def get_response_list(df_query_chuck):
        rows = []
        response = get_normalize_addresses_batch(df_query_chuck.to_dict('records'), url=url, **kwargs)
        for address in response['resultados']:
            row = {} if len(address['direcciones']) == 0 else address['direcciones'][0]
            rows.append(flatten_dict(row, prefix=kwargs.get('prefix', '')))
        return rows

    list_response = []
    for pos, df_query_chuck in enumerate(df_query_list):
        log.info(
            "Consulta {}: Registros {} a {} de {}".format(pos + 1, pos * chunk + 1, (pos + 1) * chunk, len(df_query))
        )
        list_response.extend(get_response_list(df_query_chuck))

    # Construye un dataframe con la lista de direcciones
    sum_response_df = pd.DataFrame(list_response)

    # Concatena los dataframe de consulta y respuesta
    result = pd.concat([df_source, sum_response_df], axis=1)

    result.to_csv(output_csv, index=False, header=True)
