from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import time


# How long videos trend in countries
class Job1:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/job1/'
    COUNTRIES = ["CA", "DE", "FR", "GB", "US"]
    start = 0
    end = 0

    def __init__(self):

        spark = SparkSession.builder.master('local').appName('progettoBD').getOrCreate()
        for country in self.COUNTRIES:
            self.start = time.time()
            self.load_data(spark, country)
            if country == "US":
                self.end = time.time()
                print('Time ' + str(self.end - self.start))

    def load_data(self, spark, country):
        full = spark.read.format('csv')\
            .option('header', 'true')\
            .option('inferSchema', 'true')\
            .option('escape', '"')\
            .option('multiLine', 'true')\
            .option("ignoreLeadingWhiteSpace", 'true')\
            .csv(self.PATH + country + 'videos.csv')
        result = full.select("title")\
            .groupBy('title')\
            .agg(sf.count('title').alias('appearances'))\
            .orderBy('appearances', ascending=False)\
            .groupBy('appearances')\
            .agg(sf.count('appearances').alias("videos"))\
            .orderBy('appearances', ascending=True)
        result.coalesce(1).write.format('csv')\
            .option('header', 'true')\
            .mode('overwrite')\
            .csv(self.OUTPUT_FOLDER + country + ".csv")


if __name__ == "__main__":
    Job1()

