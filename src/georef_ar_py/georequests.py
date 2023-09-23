import requests as requests

API_BASE_URL = "https://apis.datos.gob.ar/georef/api/"
TOKEN = None


def get_limits(url, endpoint, **kwargs):
    headers = {'Authorization': 'Bearer {}'.format(TOKEN)} if TOKEN and url == API_BASE_URL else None
    with requests.get("{}{}".format(url, endpoint), params=kwargs, headers=headers) as req:
        store = req.headers._store
        empty = (None, None)
        return {
            'limit-day': store.get('x-ratelimit-limit-day', empty)[1],
            'remaining-day': store.get('x-ratelimit-remaining-day', empty)[1],
            'limit-hour': store.get('x-ratelimit-limit-hour', empty)[1],
            'remaining-hour': store.get('x-ratelimit-remaining-hour', empty)[1],
            'limit-minute': store.get('x-ratelimit-limit-minute', empty)[1],
            'remaining-minute': store.get('x-ratelimit-remaining-minute', empty)[1],
            'limit-second': store.get('x-ratelimit-limit-second', empty)[1],
            'remaining-second': store.get('x-ratelimit-remaining-second', empty)[1],
        }


def get_json(url, endpoint, **kwargs):
    """
        Obtiene una respuesta como json para la entidad consultada.
    :param url: URL de la API a consultar.
    :param endpoint: nombre de una de las capas.
    :param kwargs: parámetros de consulta.
    :return: Un diccionario con la respuesta obtenida
    """
    headers = {'Authorization': 'Bearer {}'.format(TOKEN)} if TOKEN and url == API_BASE_URL else None
    with requests.get("{}{}".format(url, endpoint), params=kwargs, headers=headers) as req:
        if req.status_code == 200:
            return req.json()
        raise requests.RequestException(req)


async def get_json_async(session, url, endpoint, **kwargs):
    """
        Obtiene una respuesta como json para la entidad consultada.
    :param session: un objeto aiohtpp.ClientSession.
    :param url: URL de la API a consultar.
    :param endpoint: nombre de una de las capas.
    :param kwargs: parámetros de consulta.
    :return: Un diccionario con la respuesta obtenida
    """
    headers = {'Authorization': 'Bearer {}'.format(TOKEN)} if TOKEN and url == API_BASE_URL else None
    async with session.get("{}{}".format(url, endpoint), params=kwargs, headers=headers) as req:
        if req.status == 200:
            return await req.json()
        raise req.raise_for_status()


def get_json_post(url, endpoint, data, **kwargs):
    headers = kwargs.pop('headers', {})
    headers.update({"Content-Type": "application/json"})
    if TOKEN and url == API_BASE_URL:
        headers.update({'Authorization': 'Bearer {}'.format(TOKEN)})

    with requests.post("{}{}".format(url, endpoint), json=data, headers=headers) as req:
        if req.status_code == 200:
            return req.json()
        raise req.raise_for_status()


async def get_json_post_async(session, url, endpoint, data, **kwargs):
    headers = kwargs.pop('headers', {})
    headers.update({"Content-Type": "application/json"})
    if TOKEN and url == API_BASE_URL:
        headers.update({'Authorization': 'Bearer {}'.format(TOKEN)})

    async with session.post("{}{}".format(url, endpoint), json=data, headers=headers) as req:
        if req.status == 200:
            return await req.json()
        raise req.raise_for_status()


