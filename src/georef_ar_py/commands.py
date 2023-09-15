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
from .normalization import normalize_address, csv_to_csv


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

    address_normalized = normalize_address(address, target_url, campos='basico', max=1, **kwargs)

    print(address_normalized)


@cli.command()
@click.argument('input', type=click.File('rb'))
@click.argument('output', type=click.File('wb'))
@click.option('--url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option('--token', required=False, type=str, show_default=True, default=None)
@click.option('--prefix', required=False, type=str, show_default=True, default='norm')
@click.option('--debug', is_flag=True, show_default=False)
def batch_normalize(input, output, **kwargs):
    """
    geoarpy batch normalize Commandline

    Normaliza un archivo csv con direcciones.
     Se suminstra un archivo (INPUT) y se leen las direcciones bajo el encabezado "direccion".
     Optativamente, se pueden suministrar columnas con información extra bajo los siguientes encabezados:
        "localidad_censal", "localidad", "departamento", "provincia"
    Escribe los resultados a un archivo csv (OUTPUT)

    url es el path a la API destino que se quiere consultar.
    """
    debug = kwargs.pop('debug')
    get_logger(logging.DEBUG if debug else logging.INFO)

    target_url = kwargs.pop('url')
    georequests.__dict__['TOKEN'] = kwargs.pop('token')

    prefix = kwargs.pop('prefix')

    csv_to_csv(input, output, target_url, max=1, prefix=prefix)
