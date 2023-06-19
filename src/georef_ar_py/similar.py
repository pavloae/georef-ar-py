import urllib
from src.georef_ar_py.georequests import API_BASE_URL, get_json, get_json_post


def get_similar(url, endpoint, nombre, **kwargs):
    url = url or API_BASE_URL
    kwargs["nombre"] = nombre
    url = "{}{}?{}".format(url, endpoint, urllib.parse.urlencode(kwargs))
    return get_json(url, endpoint, **kwargs)[endpoint]


def get_similar_bulk(url, endpoint, nombres):
    """Normaliza una lista de nombres de alguna de las entidades geográficas."""

    url = url or API_BASE_URL
    data = {
        endpoint: [
            {"nombre": nombre, "max": 1} for nombre in nombres
        ]}
    results = get_json_post(url, endpoint, data)

    # convierte a una lista de "resultado más probable" o "vacío" cuando no hay
    parsed_results = [
        single_result[endpoint][0] if single_result[endpoint] else {}
        for single_result in results["resultados"]
    ]

    return parsed_results
