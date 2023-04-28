import urllib

import requests as requests

API_BASE_URL = "https://apis.datos.gob.ar/georef/api/"


def get_similar(endpoint, nombre, **kwargs):
    kwargs["nombre"] = nombre
    url = "{}{}?{}".format(API_BASE_URL, endpoint, urllib.parse.urlencode(kwargs))
    return requests.get(url).json()[endpoint]


def get_similar_bulk(endpoint, nombres):
    """Normaliza una lista de nombres de alguna de las entidades geográficas."""

    # realiza consulta a la API
    data = {
        endpoint: [
            {"nombre": nombre, "max": 1} for nombre in nombres
        ]}
    url = API_BASE_URL + endpoint
    results = requests.post(
        url, json=data, headers={"Content-Type": "application/json"}
    ).json()

    # convierte a una lista de "resultado más probable" o "vacío" cuando no hay
    parsed_results = [
        single_result[endpoint][0] if single_result[endpoint] else {}
        for single_result in results["resultados"]
    ]

    return parsed_results


def get_territorial_units(ubicaciones):
    """Pide las unidades territoriales que contienen a c/punto de una lista de coordenadas."""

    # realiza consulta a la API
    endpoint = "ubicacion"
    data = {
        "ubicaciones": [
            {"lat": ubicacion["lat"], "lon": ubicacion["lon"], "aplanar": True}
            for ubicacion in ubicaciones
        ]}
    url = API_BASE_URL + endpoint

    results = requests.post(
        url, json=data, headers={"Content-Type": "application/json"}
    ).json()

    # convierte a una lista de "resultado más probable" o "vacío" cuando no hay
    parsed_results = [
        single_result[endpoint] if single_result[endpoint] else {}
        for single_result in results["resultados"]
    ]

    return parsed_results
