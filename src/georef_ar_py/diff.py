import asyncio
import json
import logging
import os
import re

import aiohttp
import pandas as pd
from deepdiff import DeepDiff
from requests import RequestException

from . import constants
from .georequests import get_json_async
from .info import get_entity_number, get_departments_ids

log = logging.getLogger(__name__)


class DiffEntity:
    def __init__(
            self, entity, src_url, target_url, entity_key=None, max_registries=None,
            ignore_order=True, significant_digits=1, exclude_paths=None, exclude_regex_paths=None, **kwargs
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
        self._limit = kwargs.get('limit', 5000)
        self._max_src = None
        self._max_target = None

    async def _get_response(self, session, url, **kwargs):
        return await get_json_async(session, url, self._entity, **kwargs)

    @property
    def max_src(self):
        if not self._max_src:
            self._max_src = get_entity_number(self._src_url, self._entity)
        return self._max_src

    @property
    def max_target(self):
        if not self._max_target:
            self._max_target = get_entity_number(self._target_url, self._entity)
        return self._max_target

    async def _get_registers(self, session, url, **kwargs):
        log.debug(f"Obteniendo registros de {url}")
        try:
            response = await self._get_response(session, url, **kwargs)
            return {entity['id']: entity for entity in response[self._entity_key]}
        except asyncio.CancelledError:
            log.error(f"Cancelando la descarga de {url}")

    async def _get_registers_by_region(self, session, url, **kwargs):
        log.debug(f"Obteniendo registros de {url}")
        limit = kwargs.get('max', self._limit)

        params = {'campos': 'completo', 'orden': 'id', 'max': limit}

        subtasks = []
        for state_id in constants.PROVINCES_DICT.keys():
            max_items_province = get_entity_number(url, self._entity, provincia=state_id)
            if max_items_province <= limit:
                log.info(f"Consultando {self._entity}:provincia[{state_id}]")
                params.pop('departamento', None)
                params.update({'provincia': state_id})
                subtasks.append(self._get_registers(session, url, **params))
            else:
                for dep_id in get_departments_ids(url, state_id):
                    log.info(f"Consultando {self._entity}:departamento[{dep_id}]")
                    params.pop('provincia', None)
                    params.update({'departamento': dep_id})
                    subtasks.append(self._get_registers(session, url, **params))

        responses = await asyncio.gather(*subtasks)

        return {key: value for response in responses for key, value in response.items()}

    async def get_src_registers(self, session):
        if not self._src_registers:
            if self.max_src < self._limit:
                self._src_registers = await self._get_registers(
                    session, self._src_url, campos="completo", orden="id", max=self._limit
                )
            else:
                self._src_registers = await self._get_registers_by_region(session, self._src_url)
        print(f"source size: {len(self._src_registers)}")
        return self._src_registers

    async def get_target_registers(self, session):
        if not self._target_registers:
            if self.max_target < self._limit:
                self._target_registers = await self._get_registers(
                    session, self._target_url, campos="completo", orden="id", max=self._limit
                )
            else:
                self._target_registers = await self._get_registers_by_region(session, self._target_url)
        print(f"target size: {len(self._target_registers)}")
        return self._target_registers

    async def _get_diff_as_dict(self):

        async with aiohttp.ClientSession() as session:
            try:
                registers = await asyncio.gather(self.get_src_registers(session), self.get_target_registers(session))
                log.info(f"Recursos descargados")
            except Exception:
                log.error(f"Descarga de recursos cancelada!")

        return DeepDiff(
            *registers,
            ignore_order=self._ignore_order, significant_digits=self._significant_digits,
            exclude_paths=self._exclude_paths, exclude_regex_paths=self._exclude_regex_paths
        )

    @property
    def diff_dict(self):
        if not self._diff_dict:
            self._diff_dict = asyncio.run(self._get_diff_as_dict())
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

        diff_df.to_csv(filename, index_label='id')

        log.debug(f"Archivo generado: {filename}")


def get_diff_object(src_url, target_url, entity):
    if entity in ['provincias', 'departamentos', 'municipios', 'asentamientos', 'localidades', 'calles']:
        return DiffEntity(entity, src_url, target_url)
    elif entity == 'localidades-censales':
        return DiffEntity(
            entity, src_url, target_url, entity_key='localidades_censales',
            exclude_regex_paths=None  # r"root\['\d+']\['centroide'\]"
        )
    raise NotImplemented(f'La capa {entity} no se encuentra implementada')


def process(src_url, target_url, layer, path_dir, ext):
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
        logging.error(f'No se pudo procesar la petición {rqe.request}')
