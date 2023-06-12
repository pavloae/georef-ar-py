import json
from typing import List

from deepdiff import DeepDiff

from src.georef_ar_py import georequests

STATES_ID = None


def get_states_ids(url):
    response = georequests.get(url, 'provincias', campos='basico', orden='id', max=24)
    return [state['id'] for state in response['provincias']]


def get_departments_ids(url, state=None):
    kwargs = {'campos': 'basico', 'orden': 'id', 'max': 5000}
    if state:
        kwargs.update({'provincia': state})
    try:
        response = georequests.get(url, 'departamentos', **kwargs)
        return [department['id'] for department in response['departamentos']]
    except Exception as e:
        raise Exception(response)


def get_diff(old, new, ignore_order=True, significant_digits=2, exclude_paths=None, exclude_regex_paths=None):
    return json.loads(
        DeepDiff(
            old, new,
            ignore_order=ignore_order, significant_digits=significant_digits,
            exclude_paths=exclude_paths, exclude_regex_paths=exclude_regex_paths
        ).to_json()
    )


def get_provinces_data_diff(old_url, new_url):
    old_count = georequests.get(old_url, 'provincias', campos='basico')['total']
    old_response = georequests.get(old_url, 'provincias', campos='completo', orden='id', max=old_count)
    old_dict = {state['id']: state for state in old_response['provincias']}

    new_count = georequests.get(new_url, 'provincias', campos='basico')['total']
    new = georequests.get(new_url, 'provincias', campos='completo', orden='id', max=new_count)
    new_states = {state['id']: state for state in new['provincias']}
    diff_dict = {
        'old_url': old_url,
        'old_registers': old_count,
        'new_url': new_url,
        'new_registers': new_count
    }
    diff_dict.update(get_diff(old_dict, new_states))
    return diff_dict


def get_departaments_data_diff(old_url, new_url):
    old_count = georequests.get(old_url, 'departamentos', campos='basico')['total']
    old_response = georequests.get(old_url, 'departamentos', campos='completo', orden='id', max=old_count)
    old_dict = {departament['id']: departament for departament in old_response['departamentos']}

    new_count = georequests.get(new_url, 'departamentos', campos='basico')['total']
    new_response = georequests.get(new_url, 'departamentos', campos='completo', orden='id', max=new_count)
    new_dict = {departament['id']: departament for departament in new_response['departamentos']}

    diff_dict = {
        'old_url': old_url,
        'old_registers': old_count,
        'new_url': new_url,
        'new_registers': new_count
    }
    exclude_regex_paths = r"root\['\d+']\['fuente'\]"
    diff_dict.update(get_diff(old_dict, new_dict))
    return diff_dict


def get_municipalities_data_diff(old_url, new_url):
    old_count = georequests.get(old_url, 'municipios', campos='basico')['total']
    old_response = georequests.get(old_url, 'municipios', campos='completo', orden='id', max=old_count)
    old_dict = {municipality['id']: municipality for municipality in old_response['municipios']}

    new_count = georequests.get(new_url, 'municipios', campos='basico')['total']
    new_response = georequests.get(new_url, 'municipios', campos='completo', orden='id', max=new_count)
    new_dict = {municipality['id']: municipality for municipality in new_response['municipios']}

    diff_dict = {
        'old_url': old_url,
        'old_registers': old_count,
        'new_url': new_url,
        'new_registers': new_count
    }
    diff_dict.update(get_diff(old_dict, new_dict))
    return diff_dict


def get_census_localities_data_diff(old_url, new_url):
    old_count = georequests.get(old_url, 'localidades-censales', campos='basico')['total']
    old_response = georequests.get(old_url, 'localidades-censales', campos='completo', orden='id', max=old_count)
    old_dict = {census_locality['id']: census_locality for census_locality in old_response['localidades_censales']}

    new_count = georequests.get(new_url, 'localidades-censales', campos='basico')['total']
    new_response = georequests.get(new_url, 'localidades-censales', campos='completo', orden='id', max=new_count)
    new_dict = {census_locality['id']: census_locality for census_locality in new_response['localidades_censales']}

    diff_dict = {
        'old_url': old_url,
        'old_registers': old_count,
        'new_url': new_url,
        'new_registers': new_count
    }
    diff_dict.update(get_diff(old_dict, new_dict))
    return diff_dict


def get_settlements_data_diff(old_url, new_url):

    crunch_size = 5000

    old_count = georequests.get(old_url, 'asentamientos', campos='basico')['total']
    total_requests = old_count // crunch_size + 1

    old_dict = {}
    for nr in range(total_requests):
        if nr == 2:  # La suma de los parámetros max e inicio debe ser menor o igual a 10000 ¿?
            break
        old_response = georequests.get(
            old_url, 'asentamientos', campos='completo', orden='id', max=crunch_size, inicio=crunch_size * nr
        )
        old_dict.update({census_locality['id']: census_locality for census_locality in old_response['asentamientos']})

    new_count = georequests.get(new_url, 'asentamientos', campos='basico')['total']
    total_requests = new_count // crunch_size + 1

    new_dict = {}
    for nr in range(total_requests):
        if nr == 2:  # La suma de los parámetros max e inicio debe ser menor o igual a 10000 ¿?
            break
        new_response = georequests.get(
            new_url, 'asentamientos', campos='completo', orden='id', max=crunch_size, inicio=crunch_size * nr
        )
        new_dict.update({census_locality['id']: census_locality for census_locality in new_response['asentamientos']})

    diff_dict = {
        'old_url': old_url,
        'old_registers': old_count,
        'new_url': new_url,
        'new_registers': new_count
    }
    diff_dict.update(get_diff(old_dict, new_dict))
    return diff_dict


def get_localities_data_diff(old_url, new_url):
    old_count = georequests.get(old_url, 'localidades', campos='basico')['total']
    old_response = georequests.get(old_url, 'localidades', campos='completo', orden='id', max=old_count)
    old_dict = {municipality['id']: municipality for municipality in old_response['localidades']}

    new_count = georequests.get(new_url, 'localidades', campos='basico')['total']
    new_response = georequests.get(new_url, 'localidades', campos='completo', orden='id', max=new_count)
    new_dict = {municipality['id']: municipality for municipality in new_response['localidades']}

    diff_dict = {
        'old_url': old_url,
        'old_registers': old_count,
        'new_url': new_url,
        'new_registers': new_count
    }
    diff_dict.update(get_diff(old_dict, new_dict))
    return diff_dict


def get_streets_data_diff(old_url, new_url):

    def get_streets_quantity(url, **kwargs):
        try:
            return georequests.get(url, 'calles', campos='basico', **kwargs)['total']
        except Exception as e:
            return -1

    def get_region_street(url, **kwargs):
        kwargs.pop('campos', None)
        kwargs.pop('orden', None)
        kwargs.pop('max', None)
        try:
            response = None
            response = georequests.get(url, 'calles', campos='basico', **kwargs)
            count = response['total']
            response = georequests.get(url, 'calles', campos='completo', orden='id', max=count, **kwargs)
            return {calle['id']: calle for calle in response['calles']}
        except Exception as e:
            return {'error': response}

    diff_dict = {
        'old_url': old_url,
        'old_registers': get_streets_quantity(old_url),
        'new_url': new_url,
        'new_registers': get_streets_quantity(new_url),
        'provincias': {}
    }

    for state_id in get_states_ids(old_url):
        print(f'Procesando la provincia {state_id}')
        print("===================================")

        diff_dict['provincias'].update({state_id: {'old_registers': None, 'new_registers': None}})

        old_state_streets_quantity = get_streets_quantity(old_url, provincia=state_id)
        diff_dict['provincias'][state_id].update({'old_registers': old_state_streets_quantity})

        new_state_streets_quantity = get_streets_quantity(new_url, provincia=state_id)
        diff_dict['provincias'][state_id].update({'new_registers': new_state_streets_quantity})

        if old_state_streets_quantity <= 5000 and new_state_streets_quantity <= 5000:
            old_dict = get_region_street(old_url, provincia=state_id)
            new_dict = get_region_street(new_url, provincia=state_id)
            diff_dict['provincias'][state_id].update(get_diff(old_dict, new_dict))

        else:
            for dep_id in get_departments_ids(old_url, state_id):
                print(f'Procesando el departamento {dep_id}')
                diff_dict['provincias'][state_id].update(
                    {dep_id: {
                        'old_registers': get_streets_quantity(old_url, departamento=dep_id),
                        'new_registers': get_streets_quantity(new_url, departamento=dep_id)
                    }}
                )
                old_dict = get_region_street(old_url, departamento=dep_id)
                new_dict = get_region_street(new_url, departamento=dep_id)
                diff_dict['provincias'][state_id][dep_id].update(get_diff(old_dict, new_dict))

    return diff_dict


def generate_report_diff(old_url, new_url, entity='all'):

    def get_state_entity(old_url, new_url, entity):
        if entity == 'provincias':
            return get_provinces_data_diff(old_url, new_url)
        elif entity == 'departamentos':
            return get_departaments_data_diff(old_url, new_url)
        elif entity == 'municipios':
            return get_municipalities_data_diff(old_url, new_url)
        elif entity == 'localidades-censales':
            return get_census_localities_data_diff(old_url, new_url)
        elif entity == 'asentamientos':
            return get_settlements_data_diff(old_url, new_url)
        elif entity == 'localidades':
            return get_localities_data_diff(old_url, new_url)
        elif entity == 'calles':
            return get_streets_data_diff(old_url, new_url)
        else:
            raise NotImplemented()

    entities = entity if entity != 'all' else [
        'provincias', 'departamentos', 'municipios', 'localidades-censales', 'asentamientos', 'localidades', 'calles'
    ]

    if isinstance(entities, str):
        entities = [entity]

    for entity in entities:
        with open(f'diff_{entity}.json', '+w') as f:
            json.dump(get_state_entity(old_url, new_url, entity), f)


