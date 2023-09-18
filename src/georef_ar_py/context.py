import json
import os
import time


class Report:  # pylint: disable=attribute-defined-outside-init
    """Representa un reporte (texto y datos) sobre la ejecución de un proceso.
    El reporte contiene logs, con fechas, de los eventos sucedidos durante la
    ejecución del proceso, así también como datos recolectados a la vez.

    Attributes:
        _logger (logging.Logger): Logger interno a utilizar.
        _logger_stream (io.StringIO): Contenidos del logger en forma de str.
        _errors (int): Cantidad de errores registrados.
        _warnings (int): Cantidad de advertencias registradas.
        _filename_base (str): Nombre base para el archivo de reporte de texto y
            el archivo de datos.
        _data (dict): Datos varios de la ejecución del proceso.

    """

    def __init__(self, logger, logger_stream=None):
        """Inicializa un objeto de tipo 'Report'.

        Args:
            logger (logging.Logger): Ver atributo '_logger'.
            logger_stream (io.StringIO): Ver atributo '_logger_stream'.

        """
        self._logger = logger
        self._logger_stream = logger_stream
        self.reset()

    def info(self, *args):
        """Agrega un registro 'info' al logger del reporte.

        Args:
            *args: Valores del registro.

        """
        self._logger.info(*args)

    def warn(self, *args):
        """Agrega un registro 'warning' al logger del reporte.

        Args:
            *args: Valores del registro.

        """
        self._warnings += 1
        self._logger.warning(*args)

    def error(self, *args):
        """Agrega un registro 'error' al logger del reporte.

        Args:
            *args: Valores del registro.

        """
        self._errors += 1
        self._logger.error(*args)

    def exception(self, *args):
        """Agrega un registro 'error' al logger del reporte, a partir de una
        excepción generada.

        Args:
            *args: Valores del registro.

        """
        self._errors += 1
        self._logger.exception(*args)

    def reset(self):
        """Reestablece el estado interno del reporte."""
        self._errors = 0
        self._warnings = 0
        self._indent = 0
        self._filename_base = time.strftime('georef-etl-%Y.%m.%d-%H.%M.%S.{}')
        self._data = {}
        self._process_registry = {}

    def write(self, dirname):
        """Escribe los contenidos del reporte (texto y datos) a dos archivos
        dentro de un directorio específico.

        Args:
            dirname (str): Directorio donde almacenar los archivos.

        """
        os.makedirs(dirname, exist_ok=True, mode=0o700)
        filename_json = self._filename_base.format('json')
        filename_txt = self._filename_base.format('txt')

        if self._logger_stream:
            with open(os.path.join(dirname, filename_txt), 'w') as f:
                f.write(self._get_report_txt())

        with open(os.path.join(dirname, filename_json), 'w') as f:
            json.dump(self._get_report_json(), f, ensure_ascii=False, indent=4)

    @property
    def logger(self):
        return self._logger


class Context:
    """Representa un contexto de ejecución que incluye logging y más.

    Attributes:
        _report (Report): Reporte actual.
    """

    def __init__(self, report):
        """Inicializa un objeto de tipo 'Context'.

        Args:
            report (Report):  Ver atributo '_report'.
        """
        self._report = report

    @property
    def report(self):
        return self._report
