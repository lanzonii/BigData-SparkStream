# Importações do Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Biblioteca Request para os dados da API
import requests

# Importação dos outros arquivos
## Parâmetros: informações do banco de dados e da API
from params import *
## Metadata: informações das tabelas
from metadata import *

# Função para pegar dados da API
def fetch_api_data(money):
    response = requests.get(f'https://economia.awesomeapi.com.br/json/daily/{money}-BRL/15')
    data = response.json()
    return data

# Função de processamento de dados
def process_batch(df, epoch_id):
    try:
        # For para executar 1 vez para cada moeda
        for i in ['EUR', 'USD']:
            # Pegando os dados da API
            raw_data = fetch_api_data(i)
            items = raw_data.items()
            
            # Aplicando a função de tratamento em todos os dados
            rows = [[func(item[key]) for key, func in row_metadata.items()] for item in items]
        
            # Salvando no banco de dados
            spark = SparkSession.builder.config("spark.jars", "./postgresql-42.2.18.jar").getOrCreate()
            new_df = spark.createDataFrame(rows, table_metadata)
            new_df.write.format("jdbc").options(**POSTGRES_CONFIG).mode("append").save()
            print(f"Dados inseridos: {rows[0][-2]}")  # Log do timestamp
 
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
 

if __name__ == "__main__":
    # Sessão do Spark
    spark = SparkSession.builder \
        .appName("EuroDollarDolar") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()
    
    # Criação de um DataFrame base para o Stream
    dolar_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    
    # Lendo e salvando os dados no banco de dados automaticamente
    query = dolar_df.writeStream \
        .trigger(processingTime='1 minute') \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()
 
    query.awaitTermination()