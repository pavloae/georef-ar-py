import asyncio
import aiohttp
import logging

from .georequests import get_json_async, get_json
from .constants import ENTITIES, PROVINCES_DICT

log = logging.getLogger(__name__)


async def _get_response(session, url, entity, **kwargs):
    try:
        return await get_json_async(session, url, entity, **kwargs)
    except asyncio.CancelledError:
        log.error(f"Cancelando la descarga de {url}")


def get_entity_number(url, entity, **kwargs):
    log.debug(f"Consultando cantidad de registros en {entity}")
    if entity not in ENTITIES:
        raise NotImplemented(f"La entidad \"{entity}\" no se encuentra implementada.")
    kwargs.update({'campos': 'basico', 'max': 1})
    response = get_json(url, entity, **kwargs)
    return response['total']


def get_departments_ids(url, state=None):
    params = {'campos': 'basico', 'orden': 'id', 'max': 5000}
    if state:
        params.update({'provincia': state})
    response = get_json(url, 'departamentos', **params)
    return [department['id'] for department in response['departamentos']]


async def _get_items(session, url, entity, **kwargs):
    entity_key = 'localidades_censales' if entity == 'localidades-censales' else entity
    params = {'campos': 'completo', 'orden': 'id', 'max': kwargs.get('max', 5000)}
    response = await _get_response(session, url, entity, **params)
    return response[entity_key]


async def _get_items_by_region(session, url, entity, **kwargs):

    limit = kwargs.get('max', 5000)

    params = {'campos': 'completo', 'orden': 'id', 'max': limit}

    subtasks = []
    for state_id in PROVINCES_DICT.keys():
        max_items_province = get_entity_number(url, entity, provincia=state_id)
        if max_items_province <= limit:
            log.info(f"Consultando {entity}:provincia[{state_id}]")
            params.pop('departamento', None)
            params.update({'provincia': state_id})
            subtasks.append(_get_response(session, url, entity, **params))
        else:
            department_ids = get_departments_ids(url, state_id)
            for dep_id in department_ids:
                log.info(f"Consultando {entity}:departamento[{dep_id}]")
                params.pop('provincia', None)
                params.update({'departamento': dep_id})
                subtasks.append(_get_response(session, url, entity, **params))

    responses = await asyncio.gather(*subtasks)

    return [row for response in responses for row in response[entity]]


async def fetch_entities(url):
    entities = {}
    async with aiohttp.ClientSession() as session:
        tasks = []
        for entity in ENTITIES:
            entities.setdefault(entity, {})
            if entity in ['asentamientos', 'calles']:
                tasks.append(asyncio.ensure_future(_get_items_by_region(session, url, entity)))
            else:
                log.info(f"Consultando {entity}")
                tasks.append(asyncio.ensure_future(_get_items(session, url, entity)))
        response = await asyncio.gather(*tasks)
        return {key: value for key, value in zip(ENTITIES, response)}


async def get_resume(url):

    log.info("Consultando datos...")

    entities = await fetch_entities(url)

    log.info("Estructurando datos...")

    resume = {}

    resume.setdefault('provincias', {}).update({'total': len(entities['provincias'])})
    resume['provincias'].update({row['id']: {} for row in entities['provincias']})

    resume.setdefault('departamentos', {}).update({'total': len(entities['departamentos'])})
    for row in entities['departamentos']:
        resume['provincias'][row['provincia']['id']].setdefault('departamentos', 0)
        resume['provincias'][row['provincia']['id']]['departamentos'] += 1

    resume.setdefault('municipios', {}).update({'total': len(entities['municipios'])})
    for row in entities['municipios']:
        resume['provincias'][row['provincia']['id']].setdefault('municipios', 0)
        resume['provincias'][row['provincia']['id']]['municipios'] += 1

    resume.setdefault('localidades-censales', {}).update({'total': len(entities['localidades-censales'])})
    for row in entities['localidades-censales']:
        resume['provincias'][row['provincia']['id']].setdefault('localidades-censales', 0)
        resume['provincias'][row['provincia']['id']]['localidades-censales'] += 1

    resume.setdefault('asentamientos', {}).update({'total': len(entities['asentamientos'])})
    for row in entities['asentamientos']:
        resume['provincias'][row['provincia']['id']].setdefault('asentamientos', 0)
        resume['provincias'][row['provincia']['id']]['asentamientos'] += 1

    resume.setdefault('localidades', {}).update({'total': len(entities['localidades'])})
    for row in entities['localidades']:
        resume['provincias'][row['provincia']['id']].setdefault('localidades', 0)
        resume['provincias'][row['provincia']['id']]['localidades'] += 1

    resume.setdefault('calles', {}).update({'total': len(entities['calles'])})
    for row in entities['calles']:
        resume['provincias'][row['provincia']['id']].setdefault('calles', 0)
        resume['provincias'][row['provincia']['id']]['calles'] += 1

    return resume
