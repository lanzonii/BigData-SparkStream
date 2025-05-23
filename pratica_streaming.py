from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import pandas as pd
import json
from datetime import datetime

# Configuração do PostgreSQL
POSTGRES_CONFIG = {
    "user": "avnadmin",
    "password": "AVNS_h-Wx6WtyD8XkaDsTgo7",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://database-cultiville.e.aivencloud.com:12670/defaultdb?sslmode=require"
}
 
# Schema da API
api_schema = StructType([
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
 
def fetch_api_data():
    # Alterado para EUR-USD
    url = 'https://economia.awesomeapi.com.br/last/EUR-USD'
    response = requests.get(url)
    data = response.json()
    return data
 
def process_batch(df, epoch_id):
    try:
        raw_data = fetch_api_data()
        rows = []
       
        # Processar o par EUR-USD
        if 'EURUSD' in raw_data:
            item = raw_data['EURUSD']
            rows.append((
                'EURUSD',
                item['code'],
                item['codein'],
                item['name'],
                float(item['high']),
                float(item['low']),
                float(item['varBid']),\
                float(item['pctChange']),
                float(item['bid']),
                float(item['ask']),
                datetime.fromtimestamp(int(item['timestamp'])),
                datetime.strptime(item['create_date'], "%Y-%m-%d %H:%M:%S")  # <- verifique o formato!
            ))
       
        if rows:
            spark = SparkSession.builder.config("spark.jars", "./postgresql-42.2.18.jar").getOrCreate()
            new_df = spark.createDataFrame(rows, api_schema)
           
            new_df.write.format("jdbc").options(
                url=POSTGRES_CONFIG["url"],
                
                dbtable="currency_quotes",
                user=POSTGRES_CONFIG["user"],
                password=POSTGRES_CONFIG["password"],
                driver=POSTGRES_CONFIG["driver"]
            ).mode("append").save()
           
            print(f"Dados inseridos: {rows[0][-2]}")  # Log do timestamp
 
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
 
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EuroDollarDolar") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()
 
    # Configurar stream com trigger de 1 minuto
    dolar_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
 
    query = dolar_df.writeStream \
        .trigger(processingTime='1 minute') \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()
 
    query.awaitTermination()