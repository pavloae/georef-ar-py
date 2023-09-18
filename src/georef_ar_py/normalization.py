import logging

from requests import HTTPError

from .exceptions import AddressNormalizationException
from .georequests import API_BASE_URL, get_json, get_json_post
import pandas as pd

from .utils import flatten_dict

log = logging.getLogger(__name__)


def get_normalized_address(address, url=API_BASE_URL, **kwargs):
    try:
        response = get_json(url, 'direcciones', direccion=address, **kwargs)
        normalized_addresses = response.get('direcciones', [])
        return {} if len(address) == 0 else normalized_addresses[0]
    except HTTPError as re:
        log.error("La consulta falló! status_code: {} - reason: {}".format(re.response.status_code, re.response.reason))
        raise AddressNormalizationException(address, re.response)


async def get_normalize_addresses_batch(addresses, url=API_BASE_URL, **kwargs):
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
                normalized_addresses.append(get_normalized_address(address, url, **kwargs))
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
