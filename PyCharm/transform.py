import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class Transform:
    def __init__(self,spark):
        self.spark=spark

    def transform_data(self,df):
        print("Transform the json file")

        split_data = split(df['value'], ':')

        df1 = df.withColumn('Stats', split_data.getItem(0)) \
            .withColumn('Ans', split_data.getItem(1)) \
            .dropna(subset=['Ans']) \
            .drop("value")

        df2 = df1.withColumn('Ans', regexp_replace('Ans', '\"', ''))\
                .withColumn('Ans', regexp_replace('Ans', '\ ', ''))\
                .withColumn('Ans', regexp_replace('Ans', '\,', ''))\
                .withColumn('Stats', regexp_replace('Stats', '\"', ''))\
                .withColumn('Stats', regexp_replace('Stats', '\ ', ''))
        return df2
