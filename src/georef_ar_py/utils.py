import json

import geopandas as gpd
from tqdm import tqdm


def flatten_dict(dd, separator='_', prefix=''):
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}


def converter(source, target):
    if source.split(".")[-1] == "ndjson" and target.split(".")[-1] == "geojson":
        ndjson2geojson(source, target)
    else:
        gdf = gpd.read_file(source)
        gdf.to_file(target)


def ndjson2geojson(source, target=None, flatten=True):
    if not target:
        target = "{}.geojson".format(source.split('.')[:-1][0])

    def open_file(file_name):
        with open(file_name, 'w') as f:
            f.write('{"type": "FeatureCollection","features": [')

    def append_to_file(data, file_name):
        with open(file_name, 'a') as f:
            f.write(data)

    def close_file(file_name):
        with open(file_name, 'a') as f:
            f.write(']}')

    with open(source, 'r') as sf:

        open_file(target)

        for number, line in enumerate(tqdm(sf.readlines())):
            if number == 0:
                continue

            text = "" if number == 1 else ", "

            entity = json.loads(line)
            geometry = entity['geometria']
            del(entity['geometria'])

            if flatten:
                entity = flatten_dict(entity)
                geometry = flatten_dict(geometry)

            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": entity
            }

            text = text + json.dumps(feature)
            append_to_file(text, target)

        close_file(target)
