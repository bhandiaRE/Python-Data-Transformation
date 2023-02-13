import data
import transform
import load
from pyspark.sql import SparkSession

class Pipeline:

    def run_pipeline(self):
        print("Running Pipeline")
        data_process = data.Data(self.spark)
        df = data_process.data()
        #df.show()
        transform_process = transform.Transform(self.spark)
        transform_df = transform_process.transform_data(df)
        #transform_df.show()
        load_process = load.Load(self.spark)
        load_process.insert_into_sql(transform_df)
        return

    def spar_session(self):
        self.spark = SparkSession.builder.appName("Pycharm").getOrCreate()

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.spar_session()
    pipeline.run_pipeline()
