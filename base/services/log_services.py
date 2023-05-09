import logging


def config_log(level=logging.INFO):
    format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
    logging.basicConfig(
        format=format,
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S')
