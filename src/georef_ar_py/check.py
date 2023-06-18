import json
import logging
import os.path
import re

import pandas as pd
from deepdiff import DeepDiff
from requests import RequestException

from . import constants
from .georequests import get_json

log = logging.getLogger(__name__)


def get_departments_ids(url, state=None):
    kwargs = {'campos': 'basico', 'orden': 'id', 'max': 5000}
    if state:
        kwargs.update({'provincia': state})

    response = {'reason': 'Unknown'}
    try:
        response = get_json(url, 'departamentos', **kwargs)
        return [department['id'] for department in response['departamentos']]
    except KeyError:
        raise RequestException(response)


class DiffEntity:
    def __init__(
            self, entity, src_url, target_url, entity_key=None, max_registries=None,
            ignore_order=True, significant_digits=1, exclude_paths=None, exclude_regex_paths=None
    ) -> None:
        super().__init__()
        self._entity = entity
        self._src_url = src_url
        self._target_url = target_url
        self._entity_key = entity_key or entity
        self._max_registries = max_registries
        self._ignore_order = ignore_order
        self._significant_digits = significant_digits
        self._exclude_paths = exclude_paths
        self._exclude_regex_paths = exclude_regex_paths
        self._src_registers = None
        self._target_registers = None
        self._diff_dict = None

    def _get_total_entity_number(self, url, **kwargs):
        return self._get_response(url, campos='basico', max=1, **kwargs)['total']

    def _get_max_entity_number(self):
        return max(self._get_total_entity_number(self._src_url), self._get_total_entity_number(self._target_url))

    @property
    def max_registries(self):
        if not self._max_registries:
            self._max_registries = self._get_max_entity_number()
        return self._max_registries

    def _get_registers_by_region(self, url, **kwargs):
        log.debug(f"Obteniendo registros de {url}")
        self._max_registries = 5000
        sum_dict = {}
        for state_id in constants.PROVINCES_DICT.keys():
            max_streets_province = self._get_total_entity_number(url, provincia=state_id)
            if max_streets_province <= 5000:
                part_dict = self._get_response(
                    url, campos='completo', orden='id', max=self.max_registries,
                    provincia=state_id, **kwargs
                )
                sum_dict.update({entity['id']: entity for entity in part_dict[self._entity_key]})
            else:
                for dep_id in get_departments_ids(url, state_id):
                    part_dict = self._get_response(
                        url, campos='completo', orden='id', max=self.max_registries,
                        departamento=dep_id, **kwargs
                    )
                    sum_dict.update({entity['id']: entity for entity in part_dict[self._entity_key]})

        return sum_dict

    def _get_response(self, url, **kwargs):
        return get_json(url, self._entity, campos='completo', orden='id', max=self.max_registries, **kwargs)

    def _get_registers(self, url, **kwargs):
        log.debug(f"Obteniendo registros de {url}")
        return {entity['id']: entity for entity in self._get_response(url, **kwargs)[self._entity_key]}

    @property
    def src_registers(self, **kwargs):
        if not self._src_registers:
            self._src_registers = self._get_registers(self._src_url, **kwargs)
        return self._src_registers

    @property
    def target_registers(self, **kwargs):
        if not self._target_registers:
            self._target_registers = self._get_registers(self._target_url, **kwargs)
        return self._target_registers

    def _get_diff_as_dict(self):
        return DeepDiff(
            self.src_registers, self.target_registers,
            ignore_order=self._ignore_order, significant_digits=self._significant_digits,
            exclude_paths=self._exclude_paths, exclude_regex_paths=self._exclude_regex_paths
        )

    @property
    def diff_dict(self):
        if not self._diff_dict:
            self._diff_dict = self._get_diff_as_dict()
        return self._diff_dict

    def diff_as_json(self, filename):
        with open(filename, '+w') as f:
            json.dump(json.loads(self.diff_dict.to_json()), f)
            log.debug(f"Archivo generado: {filename}")

    def diff_as_csv(self, filename):

        diff_dict = {}

        def get_keys(string):
            return [str(k).replace("['", "").replace("']", "") for k in re.findall(r'\[.*?\]', string)]

        for row in self.diff_dict.get('dictionary_item_added', []):
            diff_dict.setdefault(get_keys(row)[0], {'added': True, 'removed': False})

        for row in self.diff_dict.get('dictionary_item_removed', []):
            diff_dict.setdefault(get_keys(row)[0], {'added': False, 'removed': True})

        for key, value in self.diff_dict.get('type_changes', {}).items():
            keys = get_keys(key)
            id = keys[0]
            head = ":".join(keys[1:])
            diff_dict.setdefault(id, {'added': False, 'removed': False}).update(
                {head + '(new)': value['new_value'], head + '(old)': value['old_value']}
            )

        for key, value in self.diff_dict.get('values_changed', {}).items():
            keys = get_keys(key)
            id = keys[0]
            head = ":".join(keys[1:])
            diff_dict.setdefault(id, {'added': False, 'removed': False}).update(
                {head + '(new)': value['new_value'], head + '(old)': value['old_value']}
            )

        if 'provincias' in self.diff_dict.keys():
            for pid in self.diff_dict['provincias'].keys():
                if 'values_changed' in self.diff_dict['provincias'][pid].keys():
                    for key, value in self.diff_dict['provincias'][pid]['values_changed'].items():
                        keys = get_keys(key)
                        id = keys[0]
                        head = ":".join(keys[1:])
                        diff_dict.setdefault(id, {'added': False, 'removed': False}).update(
                            {head + '(new)': value['new_value'], head + '(old)': value['old_value']}
                        )

        diff_df = pd.DataFrame.from_dict(diff_dict, orient='index')

        diff_df.to_csv(filename)

        log.debug(f"Archivo generado: {filename}")


class DiffSettlement(DiffEntity):

    def _get_registers(self, url, **kwargs):
        return self._get_registers_by_region(url, **kwargs)


class DiffStreet(DiffEntity):

    def _get_registers(self, url, **kwargs):
        return self._get_registers_by_region(url, **kwargs)


def get_diff_entity(src_url, target_url, entity):
    if entity in ['provincias', 'departamentos', 'municipios', 'localidades']:
        return DiffEntity(entity, src_url, target_url)
    elif entity == 'localidades-censales':
        return DiffEntity(
            entity, src_url, target_url, entity_key='localidades_censales',
            exclude_regex_paths=r"root\['\d+']\['centroide'\]"
        )
    elif entity == 'asentamientos':
        return DiffSettlement(entity, src_url, target_url)
    elif entity == 'calles':
        return DiffStreet(entity, src_url, target_url)
    raise NotImplemented(f'La capa {entity} no se encuentra implementada')


def generate_report_diff(path_dir, src_url, target_url, layer='all', ext=None):

    if ext and ext not in ['json', 'csv', 'both']:
        raise NotImplemented(f'La extensión {ext} no está implementada.')

    entities = layer if layer != 'all' else constants.ENTITIES

    if isinstance(entities, str):
        entities = [layer]

    for layer in entities:
        log.debug(f'Procesando la capa "{layer}"')

        try:
            diff_entity = get_diff_entity(src_url, target_url, layer)
            if not ext:
                diff_entity.diff_as_json(os.path.join(path_dir, f'diff_{layer}.json'))
                diff_entity.diff_as_csv(os.path.join(path_dir, f'diff_{layer}.csv'))
            elif ext == 'json':
                diff_entity.diff_as_json(os.path.join(path_dir, f'diff_{layer}.json'))
            else:
                diff_entity.diff_as_csv(os.path.join(path_dir, f'diff_{layer}.csv'))
        except RequestException as rqe:
            log.error(f'No se pudo procesar la petición {rqe.request}')
