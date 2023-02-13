import pyspark
from pyspark.sql import SparkSession
import pyodbc
import pandas
import pandas.io.sql as sqlio


class Load:
    def __init__(self,spark):
        self.spark=spark

    def load_data(self,df):
        print("Load data")
        #df.coalesce(1).write.option("header", "true").csv("transformed_marketsales")

    def insert_into_sql(self,df):
        server = 'LAPTOP-3MOD7J3C\SQLEXPRESS'
        database = 'Sasol'
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                              SERVER=' + server + '; \
                              DATABASE=' + database + ';\
                              Trusted_Connection=yes;')
        cnxn.autocommit = True
        cursor=cnxn.cursor()
        cursor.execute('INSERT INTO [Sasol].[dbo].[SasolGarage] ("Sasol Garage Address", "namess")\
                        VALUES(?);', df)
        cursor.close()
        cnxn.close()
