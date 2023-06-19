import logging
from requests import RequestException

from .georequests import get_json
from .constants import ENTITIES, PROVINCES_DICT

log = logging.getLogger(__name__)


def _get_response(url, entity, **kwargs):
    return get_json(url, entity, **kwargs)


def get_entity_number(url, entity, **kwargs):
    log.debug(f"Consultando cantidad de registros en {entity}")
    if entity not in ENTITIES:
        raise NotImplemented(f"La entidad \"{entity}\" no se encuentra implementada.")
    kwargs.update({'campos': 'basico', 'max': 1})
    return _get_response(url, entity, **kwargs)['total']


def get_entities_number(url, **kwargs):
    entities = {}
    for entity in ENTITIES:
        try:
            entities.update({entity: get_entity_number(url, entity, **kwargs)})
        except RequestException:
            entities.update({entity: None})
    return entities


def get_departments_ids(url, state=None):
    params = {'campos': 'basico', 'orden': 'id', 'max': 5000}
    if state:
        params.update({'provincia': state})
    response = _get_response(url, 'departamentos', **params)
    return [department['id'] for department in response['departamentos']]


def _get_items(url, entity, entity_key=None, **kwargs):
    entity_key = entity_key or entity
    params = {'campos': 'completo', 'orden': 'id', 'max': kwargs.get('max', 5000)}
    response = _get_response(url, entity, **params)
    return response[entity_key]


def _get_items_by_region(url, entity, entity_key=None, **kwargs):

    entity_key = entity_key or entity
    limit = kwargs.get('max', 5000)

    params = {'campos': 'completo', 'orden': 'id', 'max': limit}

    item_list = []
    for state_id in PROVINCES_DICT.keys():
        max_items_province = get_entity_number(url, entity, provincia=state_id)
        if max_items_province <= limit:
            log.info(f"Consultando {entity}:provincia[{state_id}]")
            params.pop('departamento', None)
            params.update({'provincia': state_id})
            response = _get_response(url, entity, **params)
            item_list.extend(response[entity_key])
        else:
            for dep_id in get_departments_ids(url, state_id):
                log.info(f"Consultando {entity}:departamento[{dep_id}]")
                params.pop('provincia', None)
                params.update({'departamento': dep_id})
                response = _get_response(url, entity, **params)
                item_list.extend(response[entity_key])

    return item_list


def get_resume(url, **kwargs):

    resume = {}

    log.info(f"Consultando provincias")
    provinces_list = _get_items(url, 'provincias')
    resume.setdefault('provincias', {}).update({'total': len(provinces_list)})
    resume['provincias'].update({row['id']: {} for row in provinces_list})

    log.info(f"Consultando departamentos")
    departments_list = _get_items(url, 'departamentos')
    resume.setdefault('departamentos', {}).update({'total': len(departments_list)})
    for row in departments_list:
        resume['provincias'][row['provincia']['id']].setdefault('departamentos', 0)
        resume['provincias'][row['provincia']['id']]['departamentos'] += 1

    log.info(f"Consultando municipios")
    municipalities_list = _get_items(url, 'municipios')
    resume.setdefault('municipios', {}).update({'total': len(municipalities_list)})
    for row in municipalities_list:
        resume['provincias'][row['provincia']['id']].setdefault('municipios', 0)
        resume['provincias'][row['provincia']['id']]['municipios'] += 1

    log.info(f"Consultando localidades-censales")
    census_localities_list = _get_items(url, 'localidades-censales', entity_key='localidades_censales')
    resume.setdefault('localidades-censales', {}).update({'total': len(census_localities_list)})
    for row in census_localities_list:
        resume['provincias'][row['provincia']['id']].setdefault('localidades-censales', 0)
        resume['provincias'][row['provincia']['id']]['localidades-censales'] += 1

    log.info(f"Consultando asentamientos")
    settlements_list = _get_items_by_region(url, 'asentamientos')
    resume.setdefault('asentamientos', {}).update({'total': len(settlements_list)})
    for row in settlements_list:
        resume['provincias'][row['provincia']['id']].setdefault('asentamientos', 0)
        resume['provincias'][row['provincia']['id']]['asentamientos'] += 1

    log.info(f"Consultando localidades")
    localities_list = _get_items(url, 'localidades')
    resume.setdefault('localidades', {}).update({'total': len(localities_list)})
    for row in localities_list:
        resume['provincias'][row['provincia']['id']].setdefault('localidades', 0)
        resume['provincias'][row['provincia']['id']]['localidades'] += 1

    log.info(f"Consultando calles")
    streets_list = _get_items_by_region(url, 'calles')
    resume.setdefault('calles', {}).update({'total': len(streets_list)})
    for row in streets_list:
        resume['provincias'][row['provincia']['id']].setdefault('calles', 0)
        resume['provincias'][row['provincia']['id']]['calles'] += 1

    return resume
