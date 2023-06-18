import logging
from requests import RequestException

from . import georequests, constants

log = logging.getLogger(__name__)


def _get_response(url, entity, **kwargs):
    return georequests.get_json(url, entity, **kwargs)


def get_entity_number(url, entity):
    log.debug(f"Consultando cantidad de registros en {entity}")
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
