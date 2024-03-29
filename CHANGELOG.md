# Changelog

Todos los cambios notables del proyecto serán documentados en este archivo

El formato está basado en [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
y este proyecto adhiere a [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

## [0.0.6] - 2023-09-23

### Added

- Implementa consultas asincronas en la normalización de direcciones por lotes para disminuir el tiempo
- Lectura de csv por chunks para controla la memoria 
- Agrega un ratelimiter rudimentario para la normalización de direcciones

## [0.0.5] - 2023-09-15

### Added

- Implementa la normalización de direcciones, simple y por lote

## [0.0.4] - 2023-08-10

### Added

- Implementa asincronismo en las consultas
- Implementa multiprocesamiento en la generación de informes

## [0.0.3] - 2023-06-19

### Added

- Implementa token en headers
- Corrige argumento en la línea de comandos
- Corrige bugs en la consulta por regiones y otros
- Modularización de algunas funciones

## [0.0.2] - 2023-06-18

### Added

- Archivo CHANGELOG.
- Implementación de comandos de cli
- Funciones para comparación de dos API's y generación de reportes
- Implementación de generación de resumen de la API
- Implementación de logs

## [0.0.1] - 2023-04-28

### Added

- Dependencia a requests==2.29.
- Archivo README y LICENSE.
- Tres funciones de consulta a la API tomadas de la documentación.

[unreleased]: https://github.com/pavloae/georef-ar-py
[0.0.6]: https://github.com/pavloae/georef-ar-py/releases/tag/0.0.6
[0.0.5]: https://github.com/pavloae/georef-ar-py/releases/tag/0.0.5
[0.0.4]: https://github.com/pavloae/georef-ar-py/releases/tag/0.0.4
[0.0.3]: https://github.com/pavloae/georef-ar-py/releases/tag/0.0.3
[0.0.2]: https://github.com/pavloae/georef-ar-py/releases/tag/0.0.2
[0.0.1]: https://github.com/pavloae/georef-ar-py/releases/tag/0.0.1