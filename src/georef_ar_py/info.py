import logging
from requests import RequestException

from . import georequests, constants

log = logging.getLogger(__name__)


def get_departments_ids(url, state=None):
    kwargs = {'campos': 'basico', 'orden': 'id', 'max': 5000}
    if state:
        kwargs.update({'provincia': state})

    response = {'reason': 'Unknown'}
    try:
        response = georequests.get_json(url, 'departamentos', **kwargs)
        return [department['id'] for department in response['departamentos']]
    except KeyError:
        raise RequestException(response)


def _get_response(url, entity, **kwargs):
    return georequests.get_json(url, entity, **kwargs)


def get_entity_number(url, entity, **kwargs):
    log.debug(f"Consultando cantidad de registros en {entity}")
    kwargs.pop('campos', None)
    kwargs.pop('max', None)
    if entity not in constants.ENTITIES:
        raise NotImplemented(f"La entidad \"{entity}\" no se encuentra implementada.")
    return _get_response(url, entity, campos='basico', max=1)['total']


def get_entities_number(url):
    entities = {}
    for entity in constants.ENTITIES:
        try:
            entities.update({entity: get_entity_number(url, entity)})
        except RequestException:
            entities.update({entity: None})
    return entities


def _get_response_by_region(url, entity, entity_key=None, **kwargs):
    if not entity_key:
        entity_key = entity

    max_registries = 5000
    sum_dict = None
    for state_id in constants.PROVINCES_DICT.keys():
        max_streets_province = get_entity_number(url, entity, provincia=state_id)
        if max_streets_province <= max_registries:
            log.info(f"Consultando {entity}:provincia[{state_id}]")
            part_dict = georequests.get_json(
                url, entity, campos='completo', orden='id', max=max_registries,
                provincia=state_id, **kwargs
            )
            if not sum_dict:
                sum_dict = part_dict
            else:
                sum_dict[entity_key].extend(part_dict[entity_key])
        else:
            for dep_id in get_departments_ids(url, state_id):
                log.info(f"Consultando {entity}:departamento[{dep_id}]")
                part_dict = georequests.get_json(
                    url, entity, campos='completo', orden='id', max=max_registries,
                    departamento=dep_id, **kwargs
                )
                if not sum_dict:
                    sum_dict = part_dict
                else:
                    sum_dict[entity_key].extend(part_dict[entity_key])

    sum_dict['cantidad'] = len(sum_dict[entity_key])

    return sum_dict


def get_resume(url):

    resume = {}

    log.info(f"Consultando provincias")
    provinces = _get_response(url, 'provincias', campos='completo', max=24)
    resume.setdefault('provincias', {}).update({'total': len(provinces.get('provincias', None))})
    resume['provincias'].update(
        {row['id']: {} for row in provinces['provincias']}
    )

    log.info(f"Consultando departamentos")
    departments = _get_response(url, 'departamentos', campos='completo', max=600)
    resume.setdefault('departamentos', {}).update({'total': len(departments.get('departamentos', None))})
    for row in departments['departamentos']:
        resume['provincias'][row['provincia']['id']].setdefault('departamentos', 0)
        resume['provincias'][row['provincia']['id']]['departamentos'] += 1

    log.info(f"Consultando municipios")
    municipalities = _get_response(url, 'municipios', campos='completo', max=5000)
    resume.setdefault('municipios', {}).update({'total': len(municipalities.get('municipios', None))})
    for row in municipalities['municipios']:
        resume['provincias'][row['provincia']['id']].setdefault('municipios', 0)
        resume['provincias'][row['provincia']['id']]['municipios'] += 1

    log.info(f"Consultando localidades-censales")
    census_localities = _get_response(url, 'localidades-censales', campos='completo', max=5000)
    resume.setdefault('localidades-censales', {}).update({'total': len(census_localities.get('localidades_censales', None))})
    for row in census_localities['localidades_censales']:
        resume['provincias'][row['provincia']['id']].setdefault('localidades-censales', 0)
        resume['provincias'][row['provincia']['id']]['localidades-censales'] += 1

    log.info(f"Consultando asentamientos")
    settlements = _get_response_by_region(url, 'asentamientos')
    resume.setdefault('asentamientos', {}).update({'total': len(settlements.get('asentamientos', None))})
    for row in settlements['asentamientos']:
        resume['provincias'][row['provincia']['id']].setdefault('asentamientos', 0)
        resume['provincias'][row['provincia']['id']]['asentamientos'] += 1

    log.info(f"Consultando localidades")
    localities = _get_response(url, 'localidades', campos='completo', max=5000)
    resume.setdefault('localidades', {}).update({'total': len(localities.get('localidades', None))})
    for row in localities['localidades']:
        resume['provincias'][row['provincia']['id']].setdefault('localidades', 0)
        resume['provincias'][row['provincia']['id']]['localidades'] += 1

    log.info(f"Consultando calles")
    streets = _get_response_by_region(url, 'calles')
    resume.setdefault('calles', {}).update({'total': len(streets.get('calles', None))})
    for row in streets['calles']:
        resume['provincias'][row['provincia']['id']].setdefault('calles', 0)
        resume['provincias'][row['provincia']['id']]['calles'] += 1

    return resume
