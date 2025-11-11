import logging
import sys


def get_stdout_logger(logger_name: str, silence_other_loggers=True) -> logging.Logger:
    """
    Return a preconfigured and named stdout Logger object.

    Args:
        logger_name (str): Name of logger
        silence_other_loggers (bool, optional): Silences other loggers. Defaults to True.

    Returns:
        logging.Logger: A named Logger object
    """
    # Set general logging level for all loggers
    logging.basicConfig(level=logging.WARNING)

    if silence_other_loggers:
        # Silence all other loggers
        for name, logger in logging.root.manager.loggerDict.items():
            if name != f"{logger_name}":
                logging.getLogger(name).setLevel(logging.WARNING)

    # Use a named logger
    nb_logger = logging.getLogger(logger_name)
    nb_logger.setLevel(logging.INFO)

    # Add handler and formattter
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")

    handler.setFormatter(formatter)
    nb_logger.addHandler(handler)
    nb_logger.propagate = False

    return nb_logger
