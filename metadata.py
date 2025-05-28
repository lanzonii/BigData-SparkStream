from pyspark.sql.types import *
from datetime import datetime

def created_date(item):
    return datetime.strptime(item, "%Y-%m-%d %H:%M:%S")

def timestamp(item):
    return datetime.fromtimestamp(int(item))

row_metadata = {'code': str, 'codein': str, 'name': str, 'high': float, 'low': float, 'varBid': float, 'pctChange': float, 'bid': float, 'ask': float, 'timestamp': timestamp, 'create_date': created_date}

table_metadata = StructType([
    StructField("pair", StringType(), True),
    StructField("code", StringType(), True),
    StructField("codein", StringType(), True),
    StructField("name", StringType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("varBid", DoubleType(), True),
    StructField("pctChange", DoubleType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("timestamp_api", TimestampType(), True),
    StructField("create_date", TimestampType(), True)
])