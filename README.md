# georef-ar-py
Paquete para consultas y procesamiento de respuestas sobre la API de georef-ar

## Installation

### Install from PyPi:

`pip install georef-ar-py`

Para usar la line a de comandos:

`pip install "georef-ar-py[cli]"`


## Ejemplo de uso por linea de comando:

`geoarpy --help`
```
Usage: geoarpy [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  diff  georef-ar-py diff Commandline
  info  georef-ar-py diff Commandline

```

### Obtener un resumen de una API

```
Usage: geoarpy info [OPTIONS]

  geoarpy info Commandline

  Obtienen un resumen de los registros en la API especificada

  url es el path a la API destino que se quiere consultar.

Options:
  --url TEXT  [default: https://apis.datos.gob.ar/georef/api/]
  --debug
  --help      Show this message and exit.

```

Obtener los datos de una API propia

`geoarpy info --url "http://mi-api.ar/georef/api/"`

Obtener los datos de la API oficial (https://apis.datos.gob.ar/georef/api/)

`geoarpy info`

### Obtener las diferencias entre dos API's

```
Usage: geoarpy diff [OPTIONS] URL

  geoarpy diff Commandline

  Calcula las diferencias de registros entre dos API's. Guarda el resultado en
  archivos de formato csv y/o json.

  url es el path a la API destino que se quiere comparar.

Options:
  --origin_url TEXT               [default:
                                  https://apis.datos.gob.ar/georef/api/]
  --layer [provincias|departamentos|municipios|localidades-censales|asentamientos|localidades|calles]
                                  [default: all]
  --extension [json|csv|both]     [default: both]
  --debug
  --help                          Show this message and exit.

```

Comparar los datos de una API propia contra la API oficial

`geoarpy diff "http://mi-api.ar/georef/api/"`

Comparar los datos entre dos API's

`geoarpy diff "http://mi-api_1.ar/georef/api/" --origin-url "http://mi-api_2.ar/georef/api/"`