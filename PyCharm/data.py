import pyspark

class Data:

    def __init__(self,spark):
        self.spark=spark

    def data(self):
        print("Data From Supermarket sales")
        df = self.spark.read.text("supermarketsales.json")
        return df