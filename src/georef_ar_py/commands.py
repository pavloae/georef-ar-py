import asyncio
import concurrent.futures
import json
import logging
import os

import click

from georef_ar_py import georequests
from georef_ar_py.constants import ENTITIES
from georef_ar_py.diff import process
from georef_ar_py.georequests import API_BASE_URL
from georef_ar_py.info import get_resume
from georef_ar_py.normalization import AddressNormalizer, Address
from georef_ar_py.plotter import plot_csv_points
from georef_ar_py.utils import converter


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
@click.option(
    '--token', required=False, type=str, show_default=True, default=None,
    help="Un token para enviar en el encabezado de cada petición"
)
@click.option(
    '--layer', required=False, type=click.Choice(ENTITIES, case_sensitive=True),
    show_default=True, default=None
)
@click.option(
    '--extension', required=False, type=click.Choice(['json', 'csv', 'both'], case_sensitive=True),
    show_default=True, default="both"
)
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
@click.option(
    '--token', required=False, type=str, show_default=True, default=None,
    help="Un token para enviar en el encabezado de cada petición"
)
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
@click.argument('direccion', type=str)
@click.option('--url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option(
    '--token', required=False, type=str, show_default=True, default=None,
    help="Un token para enviar en el encabezado de cada petición"
)
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

    address = Address(**kwargs)

    address_normalizer = AddressNormalizer(target_url)

    address_normalizer.normalize(address)

    print(address.nomenclature)


@cli.command()
@click.argument('input_csv', type=click.Path(exists=True))
@click.argument('output_csv', type=click.Path(writable=True))
@click.option('--url', required=False, type=str, show_default=True, default=API_BASE_URL)
@click.option(
    '--token', required=False, type=str, show_default=True, default=None,
    help="Un token para enviar en el encabezado de cada petición"
)
@click.option(
    '--chunk_size', required=False, type=int, show_default=True, default=1000,
    help="Cantidad de registros a leer desde un csv en cada procesamiento"
)
@click.option(
    '--data_size', required=False, type=int, show_default=True, default=500,
    help="Cantidad de registros a enviar en cada petición POST"
)
@click.option(
    '--rps', required=False, type=int, show_default=True, default=None,
    help="Número máximo de peticiones por segundo"
)
@click.option('--debug', is_flag=True, show_default=False)
def batch_normalize(input_csv, output_csv, **kwargs):
    """
    geoarpy batch normalize Commandline

    Normaliza un archivo csv con direcciones.
     Se suministra un archivo (input_csv) y se leen las direcciones bajo el encabezado "direccion".
     Optativamente, se pueden suministrar columnas con información extra bajo los siguientes encabezados:
        "localidad_censal", "localidad", "departamento", "provincia"

    Escribe los resultados a un archivo csv (output_csv)
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

@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('output_file', type=click.Path(writable=True))
@click.option('--debug', is_flag=True, show_default=False)
def convert(input_file, output_file, **kwargs):
    """
    geoarpy convert Commandline

    Normaliza un archivo csv con direcciones.
     Se suministra un archivo (input_csv) y se leen las direcciones bajo el encabezado "direccion".
     Optativamente, se pueden suministrar columnas con información extra bajo los siguientes encabezados:
        "localidad_censal", "localidad", "departamento", "provincia"

    Escribe los resultados a un archivo csv (output_csv)
    """

    debug = kwargs.pop('debug')
    get_logger(logging.DEBUG if debug else logging.INFO)

    converter(input_file, output_file)


@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.option('--output_file', default=None, help='Nombre del archivo de salida.')
@click.option('--lon', required=False, type=str, show_default=True, default="lon")
@click.option('--lat', required=False, type=str, show_default=True, default="lat")
@click.option('--debug', is_flag=True, show_default=False)
def plot_file(input_file, **kwargs):
    """
    geoarpy plot-points Commandline

    Grafica un csv con puntos de coordenadas
     Se suministra un archivo (input_csv) y se leen las coordenadas
    """

    if not (output_file := kwargs.get('output_file')):
        output_file = os.path.splitext(input_file)[0] + '.png'


    debug = kwargs.pop('debug')
    get_logger(logging.DEBUG if debug else logging.INFO)

    plot_csv_points(input_file, output_file, lon_head=kwargs.get('lon'), lat_head=kwargs.get('lat'))
