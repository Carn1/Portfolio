from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

HDFS_HOST = os.getenv('HDFS_HOST')
HDFS_PORT = os.getenv('HDFS_PORT')

JDBC_DRIVER_PATH = "postgresql-42.7.4.jar"
HDFS_FILE_PATH = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/user/s.ashkinadzi/bank_transactions.csv"
TABLE_NAME = 'team_a.transactions'

spark = (SparkSession.builder
         .appName("From client_logins.csv to table client_logins_buffer")
         .config("spark.jars", JDBC_DRIVER_PATH)
         .getOrCreate())

df = spark.read.csv(HDFS_FILE_PATH, header=True, inferSchema=True)

jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

properties = {
    "user": DB_USERNAME,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}



df.write.jdbc(url=jdbc_url, table=TABLE_NAME, mode='overwrite', properties=properties)
