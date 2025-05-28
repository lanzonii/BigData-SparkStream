from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import pandas as pd
from datetime import datetime
from params import *
from metadata import *

def fetch_api_data(url):
    response = requests.get(url)
    data = response.json()
    return data

def fetch_api_data(url, complement):
    response = requests.get(url+complement)
    data = response.json()
    return data

def process_batch(df, epoch_id):
    try:
        raw_data = fetch_api_data(url, 'EUR-BRL')
        rows = []
       
        # Processar o par EUR-USD
        if 'EURBRL' in raw_data:
            item = raw_data['EURBRL']
            rows.append(['EURBRL']+[func(item[key]) for key, func in row_metadata.items()])
       
        if rows:
            spark = SparkSession.builder.config("spark.jars", "./postgresql-42.2.18.jar").getOrCreate()
            new_df = spark.createDataFrame(rows, table_metadata)
            new_df.write.format("jdbc").options(**POSTGRES_CONFIG).mode("append").save()
            print(f"Dados Euro inseridos: {rows[0][-2]}")  # Log do timestamp
        
        raw_data = fetch_api_data(url, 'USD-BRL')
        rows = []
        
        # Processar o par EUR-USD
        if 'USDBRL' in raw_data:
            item = raw_data['USDBRL']
            rows.append(['USDBRL']+[func(item[key]) for key, func in row_metadata.items()])
        
        if rows:
            spark = SparkSession.builder.config("spark.jars", "./postgresql-42.2.18.jar").getOrCreate()
            new_df = spark.createDataFrame(rows, table_metadata)
            new_df.write.format("jdbc").options(**POSTGRES_CONFIG).mode("append").save()
            print(f"Dados Dolar inseridos: {rows[0][-2]}")  # Log do timestamp
 
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
 

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EuroDollarDolar") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()
 
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