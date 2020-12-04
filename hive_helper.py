from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("hive_helper") \
    .getOrCreate()


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


def init_hive_table(init=False):
    """
    DEBUG function
    To initialize the hive table.
    :param init:
    :return:
    """
    df = sample_df(enrichment_schema, init)
    df.write.mode("append").saveAsTable(TABLE_NAME)


if __name__ == '__main__':
    TABLE_NAME = "TABLE_NAME"
    enrichment_schema = StructType([
        StructField("string_col", StringType(), False),
        StructField("long_col", LongType(), False),
        StructField("boolean_col", BooleanType(), False),
        StructField("time_stamp_col", TimestampType(), False),
    ])

    init_hive_table()
