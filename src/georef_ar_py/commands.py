import json
import logging
import os

import click
from requests import RequestException

from . import georequests
from .constants import ENTITIES
from .diff import get_diff_object
from .georequests import API_BASE_URL
from .info import get_resume


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

    for layer in entities:
        log.info(f'Procesando la capa "{layer}"')

        try:
            diff_entity = get_diff_object(src_url, target_url, layer)
            if ext == 'both':
                diff_entity.diff_as_json(os.path.join(path_dir, f'diff_{layer}.json'))
                diff_entity.diff_as_csv(os.path.join(path_dir, f'diff_{layer}.csv'))
            elif ext == 'json':
                diff_entity.diff_as_json(os.path.join(path_dir, f'diff_{layer}.json'))
            else:
                diff_entity.diff_as_csv(os.path.join(path_dir, f'diff_{layer}.csv'))
        except RequestException as rqe:
            log.error(f'No se pudo procesar la petición {rqe.request}')


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

    resume = get_resume(target_url)

    filename = os.path.join(os.getcwd(), 'info.json')
    with open(filename, '+w') as f:
        json.dump(resume, f)
