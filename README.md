# georef-ar-py
Paquete para consultas y procesamiento de respuestas sobre la API de georef-ar

## Installation

### Install from PyPi:

`pip install georef-ar-py`

Para usar desde la línea de comandos:

`pip install "georef-ar-py[cli]"`


## Ejemplo de uso por linea de comando:

`geoarpy --help`
```
Usage: geoarpy [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  batch-normalize  geoarpy batch normalize Commandline
  diff             geoarpy diff Commandline
  info             geoarpy info Commandline
  normalize        geoarpy normalize Commandline
```

### Obtener un resumen de una API
`geoarpy info --help`
```
Usage: geoarpy info [OPTIONS]

  geoarpy info Commandline

  Obtienen un resumen de los registros en la API especificada

  url es el path a la API destino que se quiere consultar.

Options:
  --url TEXT    [default: https://apis.datos.gob.ar/georef/api/]
  --token TEXT  Un token para enviar en el encabezado de cada petición
  --debug
  --help        Show this message and exit.
```

Obtener los datos de una API propia

`geoarpy info --url "http://mi-api.ar/georef/api/"`

Obtener los datos de la API oficial (https://apis.datos.gob.ar/georef/api/)

`geoarpy info`

### Obtener las diferencias entre dos API's
`geoarpy diff --help`
```
Usage: geoarpy diff [OPTIONS] URL

  geoarpy diff Commandline

  Calcula las diferencias de registros entre dos API's. Guarda el resultado en
  archivos de formato csv y/o json.

  url es el path a la API destino que se quiere comparar.

Options:
  --origin_url TEXT               [default:
                                  https://apis.datos.gob.ar/georef/api/]
  --token TEXT                    Un token para enviar en el encabezado de
                                  cada petición
  --layer [provincias|departamentos|municipios|localidades-censales|asentamientos|localidades|calles]
  --extension [json|csv|both]     [default: both]
  --debug
  --help                          Show this message and exit.
```

Comparar los datos de una API propia contra la API oficial

`geoarpy diff "http://mi-api.ar/georef/api/"`

Comparar los datos entre dos API's

`geoarpy diff "http://mi-api_1.ar/georef/api/" --origin-url "http://mi-api_2.ar/georef/api/"`

### Normalizar una dirección (Obtener la nomenclatura)
`geoarpy normalize --help`
```
Usage: geoarpy normalize [OPTIONS] ADDRESS

  geoarpy normalize Commandline

  Dada una direccion suministra la misma normalizada (nomenclatura) o no
  devuelve nada si no se pudo normalizar

  url es el path a la API destino que se quiere consultar.

Options:
  --url TEXT               [default: https://apis.datos.gob.ar/georef/api/]
  --token TEXT             Un token para enviar en el encabezado de cada
                           petición
  --provincia TEXT
  --departamento TEXT
  --localidad_censal TEXT
  --localidad TEXT
  --debug
  --help                   Show this message and exit.
```

Normaliza una dirección indicando la localidad

`geoarpy normalize "feliz san marti 390" --localidad_censal="junin del los andes"`

Output:

`FELIX SAN MARTIN 390, Huiliches, Neuquén`

### Normalizar un conjunto de direcciones
`geoarpy batch-normalize --help`
```
Usage: geoarpy batch-normalize [OPTIONS] INPUT_CSV OUTPUT_CSV

  geoarpy batch normalize Commandline

  Normaliza un archivo csv con direcciones.  Se suministra un archivo
  (input_csv) y se leen las direcciones bajo el encabezado "direccion".
  Optativamente, se pueden suministrar columnas con información extra bajo los
  siguientes encabezados:     "localidad_censal", "localidad", "departamento",
  "provincia"

  Escribe los resultados a un archivo csv (output_csv)

Options:
  --url TEXT            [default: https://apis.datos.gob.ar/georef/api/]
  --token TEXT          Un token para enviar en el encabezado de cada petición
  --chunk_size INTEGER  Cantidad de registros a leer desde un csv en cada
                        procesamiento  [default: 1000]
  --data_size INTEGER   Cantidad de registros a enviar en cada petición POST
                        [default: 500]
  --rps INTEGER         Número máximo de peticiones por segundo
  --debug
  --help                Show this message and exit.

```

Lee las direcciones de un archivo csv y escribe los datos normalizados en un nuevo archivo

`geoarpy batch-normalize ./direciones.csv ./direcciones_normalizadas.csv`
