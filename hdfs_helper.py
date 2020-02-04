
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
from datetime import datetime, timedelta
from os import path
from time import time

logger = logging.getLogger(__name__)


# # Initialize logger
# logger = logging.getLogger('hdfs_helper')
# logger.setLevel(logging.DEBUG)
#
# date_time_now = datetime.now().strftime('%Y%m%d_%H%M%S')
# logger.info("String date_time_now: {}".format(date_time_now))
#
# log_file_name = "enrich_{}.log".format(date_time_now)
# logger.info("Setting logger path to: {}".format(log_file_name))
#
# # create file handler which logs even debug messages
# fh = logging.FileHandler(log_file_name)
# fh.setLevel(logging.DEBUG)
# # create console handler with a higher log level
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# # create formatter and add it to the handlers
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
# fh.setFormatter(formatter)
# ch.setFormatter(formatter)
# # add the handlers to the logger
# logger.addHandler(fh)
# logger.addHandler(ch)
# logger.info("Program Start")
# logger.info("Logger Initialized")

spark = SparkSession \
    .builder \
    .appName("hdfs_functions") \
    .getOrCreate()

sc = spark.sparkContext

Hadoop = sc._jvm.org.apache.hadoop
Config = sc._jsc.hadoopConfiguration()

Path = Hadoop.fs.Path
FileSystem = Hadoop.fs.FileSystem
FileUtil = Hadoop.fs.FileUtil

fs = FileSystem.get(Config)


def hdfs_exists(path_):
    """
    Checks if a hdfs file exists
    Similar to os.exists or something
    :param path_: path to hdfs file
    :return: True or False
    """
    return fs.exists(Path(path_))


def hdfs_glob(path_):
    """
    Glob a directory given the path and '*'
    Similar to python glob
    :param path_: full path to search in  hdfs with '*'
    :return: List of full file path
    """
    return [filestatus.getPath().toString().encode("utf-8") for filestatus in fs.globStatus(Path(path_))]


def hdfs_list(path_, ignores_start=True):
    """
    List file in a directory. Be sure to end with /
    :param ignores_start: Where to ignore basename files that startswith '.'
    :param path_: Full path  in hdfs
    :return: List  of full file path
    """
    file_list = [filestatus.getPath().toString().encode("utf-8") for filestatus in fs.listStatus(Path(path_))]
    if ignores_start:
        return [f for f in file_list if not path.basename(f).startswith('.')]
    else:
        return file_list


def hdfs_mv(src, target):
    """
    In hadoop, move is to rename file/directory
    :param src: current location
    :param target: final destination
    :return: True or False
    """
    return fs.rename(Path(src), Path(target))


def hdfs_cp(src, target):
    """
    Copy files in hdfs. Not recommended as it takes a long time.
    :param src: current location
    :param target: final destination
    :return:  True or False
    """
    return FileUtil.copy(fs, Path(src), fs, Path(target), False, Config)


def hdfs_mkdirs(path_):
    """
    Not tested but it is to make a directory
    :param path_: new directory path
    :return: True  or False
    """
    return fs.mkdirs(Path(path_))


def hdfs_delete(path_):
    """
    To Delete file in hdfs
    :param path_: path of file to delete
    :return: True or  False
    """
    return fs.delete(Path(path_))


def hdfs_mod_time(path_):
    """
    Get python datetime modified time of a file/directory
    :param path_: Path to file/directory
    :return: Datetime of file/directory
    """
    # .strftime('%Y%m%d_%H%M%S')
    if hdfs_exists(path_):
        ts = fs.listStatus(Path(path_))[0].getModificationTime()
        return datetime.fromtimestamp(ts / 1000.0)
    else:
        return False


def hdfs_get_space(path_, size='B'):
    """
    Get total space consumed.
    If a directory/file is 1.5 GB, and 3 replica, it will return 4.5 GB
    :param path_: Path to file/directory
    :param size: file size format to return, [ B, M, K, G, T]
    :return: file size (bytes)
    """
    if hdfs_exists(path_):
        # filecount = fs.getContentSummary(Path(path_)).getFileCount()
        space_consumed = fs.getContentSummary(Path(path_)).getSpaceConsumed()
        if size == 'B':
            return space_consumed
        elif size == 'K':
           return space_consumed/1024.0
        elif size == 'M':
            return space_consumed/1024.0/1024.0
        elif size == 'G':
            return space_consumed/1024.0/1024.0/1024.0
        elif size == 'T':
            return space_consumed/1024.0/1024.0/1024.0/1024.0
    else:
        return False


def write_read_temp(df, save_path, num_part=None, max_records=None):
    start_time = time()
    # Re read and write intermediate files
    # logger.info('write_read_temp path: {}'.format(save_path))
    FORMAT_TYPE = 'parquet'

    if max_records and num_part:
        df.repartition(num_part).write.option('maxRecordsPerFile', max_records).mode('overwrite').format(FORMAT_TYPE).save(save_path)
    elif num_part:
        # logger.warning("FORCING {} REPARTITION! in write_read_temp".format(num_part))
        df.repartition(num_part).write.mode('overwrite').format(FORMAT_TYPE).save(save_path)
    else:
        df.write.mode('overwrite').format(FORMAT_TYPE).save(save_path)
    # logger.info('Intermediate Step has been saved. Beginning to Read.')
    df = spark.read.load(save_path, format=FORMAT_TYPE)
    # logger.info('Computation completed, saved and read.')
    logger.info('Time Taken : {}'.format(time() - start_time))
    return df

