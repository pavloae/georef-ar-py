from src.georef_ar_py.georequests import API_BASE_URL, get_json_post


def get_territorial_units(url, ubicaciones):
    """Pide las unidades territoriales que contienen a c/punto de una lista de coordenadas."""

    url = url or API_BASE_URL
    endpoint = "ubicacion"
    data = {
        "ubicaciones": [
            {"lat": ubicacion["lat"], "lon": ubicacion["lon"], "aplanar": True}
            for ubicacion in ubicaciones
        ]}

    results = get_json_post(url, endpoint, data)

    # convierte a una lista de "resultado más probable" o "vacío" cuando no hay
    parsed_results = [
        single_result[endpoint] if single_result[endpoint] else {}
        for single_result in results["resultados"]
    ]

    return parsed_results
