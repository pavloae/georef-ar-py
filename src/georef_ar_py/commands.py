import asyncio
import concurrent.futures
import json
import logging
import os

import click

from . import georequests
from .constants import ENTITIES
from .diff import process
from .georequests import API_BASE_URL
from .info import get_resume
from .normalization import AddressNormalizer


def get_logger(level):
    logging.basicConfig(level=level, format="%(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    return logger


@click.group()
def cli():
    pass


@cli.command()
@click.argument('url', type=str)
@click.option('--origin_url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option('--token', required=False, type=str, show_default=True, default=None)
@click.option('--layer', required=False, type=click.Choice(ENTITIES, case_sensitive=True), show_default=True, default=None)
@click.option('--extension', required=False, type=click.Choice(['json', 'csv', 'both'], case_sensitive=True), show_default=True, default="both")
@click.option('--debug', is_flag=True, show_default=False)
def diff(*args, **kwargs):
    """
    geoarpy diff Commandline

    Calcula las diferencias de registros entre dos API's.
    Guarda el resultado en archivos de formato csv y/o json.

    url es el path a la API destino que se quiere comparar.
    """
    debug = kwargs.pop('debug')
    log = get_logger(logging.DEBUG if debug else logging.INFO)
    log.info("Comenzando la generación de diferencias...")

    src_url = kwargs.pop('origin_url')
    target_url = kwargs.pop('url')
    ext = kwargs.pop('extension')
    layer = kwargs.pop('layer')
    georequests.__dict__['TOKEN'] = kwargs.pop('token')

    path_dir = os.getcwd()

    if ext and ext not in ['json', 'csv', 'both']:
        raise NotImplemented(f'La extensión {ext} no está implementada.')

    entities = layer if layer else ENTITIES

    if isinstance(entities, str):
        entities = [layer]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for entity in entities:
            executor.submit(process, src_url, target_url, entity, path_dir, ext)


@cli.command()
@click.option('--url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option('--token', required=False, type=str, show_default=True, default=None)
@click.option('--debug', is_flag=True, show_default=False)
def info(*args, **kwargs):
    """
    geoarpy info Commandline

    Obtienen un resumen de los registros en la API especificada

    url es el path a la API destino que se quiere consultar.
    """
    debug = kwargs.pop('debug')
    log = get_logger(logging.DEBUG if debug else logging.INFO)
    log.info("Comenzando la generación del resumen...")

    target_url = kwargs.pop('url')
    georequests.__dict__['TOKEN'] = kwargs.pop('token')

    resume = asyncio.run(get_resume(target_url))

    filename = os.path.join(os.getcwd(), 'info.json')
    log.info(f"Guardando archivo en {filename}")
    with open(filename, '+w') as f:
        json.dump(resume, f)


@cli.command()
@click.argument('address', type=str)
@click.option('--url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option('--token', required=False, type=str, show_default=True, default=None)
@click.option('--provincia', required=False, type=str, show_default=True, default=None)
@click.option('--departamento', required=False, type=str, show_default=True, default=None)
@click.option('--localidad_censal', required=False, type=str, show_default=True, default=None)
@click.option('--localidad', required=False, type=str, show_default=True, default=None)
@click.option('--debug', is_flag=True, show_default=False)
def normalize(*args, **kwargs):
    """
    geoarpy normalize Commandline

    Dada una direccion suministra la misma normalizada (nomenclatura) o no devuelve nada si no se pudo normalizar

    url es el path a la API destino que se quiere consultar.
    """
    debug = kwargs.pop('debug')
    get_logger(logging.DEBUG if debug else logging.INFO)

    target_url = kwargs.pop('url')
    georequests.__dict__['TOKEN'] = kwargs.pop('token')

    address = kwargs.pop('address')

    address_normalized = asyncio.run(
        AddressNormalizer.normalize_address(address, target_url, campos='basico', max=1, **kwargs)
    )

    if len(address_normalized) > 0:
        print(address_normalized['nomenclatura'])


@cli.command()
@click.argument('input_csv', type=click.Path('rb'))
@click.argument('output_csv', type=click.Path('wb'))
@click.option('--url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option('--token', required=False, type=str, show_default=True, default=None)
@click.option('--chunk_size', required=False, type=int, show_default=True, default=1000)
@click.option('--data_size', required=False, type=int, show_default=True, default=500)
@click.option('--rps', required=False, type=int, show_default=True, default=40)
@click.option('--debug', is_flag=True, show_default=False)
def batch_normalize(input_csv, output_csv, **kwargs):
    """
    geoarpy batch normalize Commandline

    Normaliza un archivo csv con direcciones.
     Se suministra un archivo (input_csv) y se leen las direcciones bajo el encabezado "direccion".
     Optativamente, se pueden suministrar columnas con información extra bajo los siguientes encabezados:
        "localidad_censal", "localidad", "departamento", "provincia"
    Escribe los resultados a un archivo csv (output_csv)

    url es el path a la API destino que se quiere consultar.
    """

    debug = kwargs.pop('debug')
    log = get_logger(logging.DEBUG if debug else logging.INFO)
    log.info("Comenzando la normalización...")

    target_url = kwargs.pop('url')
    georequests.__dict__['TOKEN'] = kwargs.pop('token')

    normalizer = AddressNormalizer(
        url=target_url,
        chunk_size=kwargs.pop('chunk_size'),
        data_size=kwargs.pop('data_size'),
        rps=kwargs.pop('rps')
    )

    normalizer.csv2csv(input_csv, output_csv)
