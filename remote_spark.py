import os
import sys
from os import path


os.environ['SPARK_HOME'] = "/PATH/TO/CLUSTER/SPARK/lib/spark/"
os.environ['PYSPARK_PYTHON'] = "/PATH/TO/CLUSTER/PYTHON/bin/python"

os.environ['PYLIB'] = path.join(os.environ['SPARK_HOME'], 'python/lib')
sys.path.append(path.join(os.environ['PYLIB'], "py4j-0.10.7-src.zip"))
sys.path.append(path.join(os.environ['PYLIB'], "pyspark.zip"))

# Set Spark config
# Stop the sparkContext and restart spark to change config
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
import pyspark.sql.functions as F

config = SparkConf().setAll([
    ("spark.driver.memory", "3g"),
    ("spark.executor.cores", "1"),
    ("spark.executor.memory", "8g"),
    ("spark.executor.instances", "30"),
    ("spark.sql.shuffle.partitions", "600"),
    ("spark.default.parallelism", "600"),
])


# Initialize spark
spark = SparkSession \
    .builder \
    .appName("remote_pyspark_1") \
    .config(conf=config) \
    .getOrCreate()
sc = spark.sparkContext


# sc.getConf().getAll()
# sc.stop()

# Test Spark
def test_spark_session():
    from pyspark.sql import SparkSession, Row
    from datetime import datetime
    def sample_df(schema, init=False):
        """
        DEBUG function
        To create an empty spark dataframe.
        :param schema:
        :param init:
        :return:
        """
        # df_names = [f.name for f in schema]
        # df_types = [f.dataType for f in schema]
        if init:
            tmp_dict = {}
            for f in schema:
                f_type = f.dataType
                if f_type == StringType():
                    value = "0"
                elif f_type == IntegerType() or f_type == LongType():
                    value = 0
                elif f_type == DoubleType():
                    value = 0.0
                elif f_type == BooleanType():
                    value = False
                elif f_type == TimestampType():
                    value = datetime.now()
                else:
                    exit("Datatype not included. Trying 0")
                    value = 0
                tmp_dict[f.name] = value
            tmp_row = Row(**tmp_dict)
            return spark.createDataFrame([tmp_row], schema=schema)
        else:
            return spark.createDataFrame([], schema=schema)

    enrichment_schema = StructType([
            StructField("string_col", StringType(), False),
            StructField("long_col", LongType(), False),
            StructField("boolean_col", BooleanType(), False),
            StructField("time_stamp_col", TimestampType(), False),
        ])

    sample_df(enrichment_schema, True).show(truncate=False)


def test_tensorflow():
    import tensorflow as tf

    # Simple hello world using TensorFlow

    # Create a Constant op
    # The op is added as a node to the default graph.
    #
    # The value returned by the constructor represents the output
    # of the Constant op.
    hello = tf.constant('Hello, TensorFlow!')

    # Start tf session
    sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))

    # Run the op
    print(sess.run(hello))

    # Creates a graph.
    a = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[2, 3], name='a')
    b = tf.constant([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], shape=[3, 2], name='b')
    c = tf.matmul(a, b)
    # Creates a session with log_device_placement set to True.
    sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))
    # Runs the op.
    print(sess.run(c))


test_spark_session()
test_tensorflow()

#### Log python and spark version
print("python version : {}".format(sys.version))
print("Current Spark Version: {}".format(spark.version))
