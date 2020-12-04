import my_helper
import logging
from datetime import datetime


def create_logger():
    # Initialize logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # create logger filename
    date_time_now = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_name = "{}_{}.log".format("logger", date_time_now)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def main():
    logger.info('this is the main')
    my_helper.hello_world()


if __name__ == '__main__':
    logger = create_logger()

    main()
