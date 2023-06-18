import logging


def get_logger(level=logging.ERROR):

    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
