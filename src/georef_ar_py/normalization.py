from .georequests import API_BASE_URL, get_json, get_json_post
import pandas as pd

from .utils import flatten_dict


def normalize_address(address, url=API_BASE_URL, **kwargs):
    response = get_json(url, 'direcciones', direccion=address, **kwargs)
    direcciones = response['direcciones']
    return '' if len(direcciones) == 0 else direcciones[0]['nomenclatura']


def normalize_address_batch(addresses_dict, url=API_BASE_URL, **kwargs):
    data = {
        'direcciones': addresses_dict
    }
    return get_json_post(url, 'direcciones', data, **kwargs)


def csv_to_csv(input_csv, output_csv, url=API_BASE_URL, **kwargs):
    df_source = pd.read_csv(input_csv).filter(items=[
        'direccion',
        'localidad_censal',
        'localidad',
        'departamento',
        'provincia'
    ]).astype('str', errors='ignore')

    df_query = df_source.copy()
    for param in ['orden', 'aplanar', 'campos', 'max', 'inicio', 'exacto']:
        if param in kwargs.keys():
            df_query[param] = kwargs.get(param)
    response = normalize_address_batch(df_query.to_dict('records'), url=url, **kwargs)

    rows = []
    prefix = kwargs.get('prefix', '')
    for address in response['resultados']:
        row = {} if len(address['direcciones']) == 0 else address['direcciones'][0]
        rows.append(flatten_dict(row, prefix=prefix))
    response_df = pd.DataFrame(rows)

    result = pd.concat([df_source, response_df], axis=1)

    result.to_csv(output_csv, index=False, header=True)
