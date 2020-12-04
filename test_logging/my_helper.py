import logging
logger = logging.getLogger(__name__)


def hello_world():
    logger.info("Hello World from helper")


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    hello_world()
