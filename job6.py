from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics
import pandas as pd
import time


# Correlation between views, likes, dislikes and comments
class Job6:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/job6/'
    start = 0
    end = 0

    def __init__(self):

        spark = SparkSession.builder.master('local').appName('progettoBD').getOrCreate()
        self.start = time.time()
        self.load_data(spark)
        self.end = time.time()
        print('Time ' + str(self.end - self.start))

    def load_data(self, spark):

        full = spark.read.format('csv')\
            .option('header', 'true')\
            .option('inferSchema', 'true')\
            .option('escape', '"')\
            .option('multiLine', 'true')\
            .option("ignoreLeadingWhiteSpace", 'true')\
            .csv(self.PATH + "*videos.csv")

        columns = full.select('views', 'likes', 'dislikes', 'comment_count')
        cols = columns.columns
        features = columns.rdd.map(lambda row: row[0:])
        corr_matrix = Statistics.corr(features)
        df = pd.DataFrame(corr_matrix)
        df.index, df.columns = cols, cols
        with open(self.OUTPUT_FOLDER + "matrix.txt", 'w') as output:
            output.write(df.to_string())

        #result.coalesce(1).write.format('csv')\
        #    .option('header', 'true')\
        #    .mode('overwrite')\
        #    .csv(self.OUTPUT_FOLDER + country + ".csv")


if __name__ == "__main__":
    Job6()

