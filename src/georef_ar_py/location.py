#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Pablo Andino
# Created Date: 2023-11-29
# version ='1.0'
# ---------------------------------------------------------------------------
""" Módulo construido para la busqueda y odenamiento de direcciones cercanas"""

import pandas as pd
import requests as requests

API_BASE_URL = "https://apis.datos.gob.ar/georef/api/"
TOKEN = None

user_address = "San Martin 150"  # input("Ingrese su dirección: ")

headers = {'Authorization': 'Bearer {}'.format(TOKEN)} if TOKEN else None
params = {
    'direccion': user_address,
    'localidad_censal': 14014010, # Suponemos que el usuario está en la localidad de Córdoba
    'max': 1,
    'campos': "nomenclatura,ubicacion"
}
response = requests.get("https://apis.datos.gob.ar/georef/api/direcciones", params=params, headers=headers)
json_response = response.json()

print("La mejor dirección encontrada es: {}".format(json_response['direcciones'][0]['nomenclatura']))
lat = json_response['direcciones'][0]['ubicacion']['lat']
lon = json_response['direcciones'][0]['ubicacion']['lon']
user_location = (lat, lon)

max_dist = float(input("Ingrese una distancia máxima [km]: "))

df = pd.read_csv('sucursales_bna.csv')
df = df[df['PROVINCIA'] == 'Córdoba']

addresses = [{"direccion": branch.DIRECCION, "campos": "nomenclatura, ubicacion", "max": 1} for branch in df.itertuples]

data = {'direcciones': addresses}
response = requests.post("https://apis.datos.gob.ar/georef/api/direcciones", json=data)
results = response.json()['resultados']
branches = [result['direcciones'][0] for result in results if len(result['direcciones']) > 0]

from haversine import haversine as hs

dist_to_user = lambda address: hs(user_location, (address['ubicacion']['lat'], address['ubicacion']['lon']))

near_branches = [{'nomenclatura': branch['nomenclatura'], 'dist': dist_to_user(branch)} for branch in branches if dist_to_user(branch) < max_dist]
