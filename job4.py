from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import time


# Best day and time to publish video by country
class Job4:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/job4/'
    COUNTRIES = ["CA", "DE", "FR", "GB", "US"]
    start1 = 0
    start2 = 0
    start3 = 0
    end1 = 0
    end2 = 0
    end3 = 0
    common_start = 0
    common_end = 0

    def __init__(self):

        spark = SparkSession.builder.master('local').appName('progettoBD').getOrCreate()

        for country in self.COUNTRIES:
            self.common_start = time.time()
            self.load_data(spark, country)
            delay = self.common_end - self.common_start
            if country == "US":
                print('Time 1 ' + str(delay + self.end1 - self.start1))
                print('Time 2 ' + str(delay + self.end2 - self.start2))
                print('Time 3 ' + str(delay + self.end3 - self.start3))

    def load_data(self, spark, country):
        full = spark.read.format('csv')\
            .option('header', 'true')\
            .option('inferSchema', 'true')\
            .option('escape', '"')\
            .option('multiLine', 'true')\
            .option("ignoreLeadingWhiteSpace", 'true')\
            .csv(self.PATH + country + 'videos.csv')

        new_full = full.withColumn('publish_day', sf.dayofweek(sf.col('publish_time')))\
            .withColumn('publish_hour', sf.hour(sf.col('publish_time')))

        self.common_end = time.time()
        self.start1 = time.time()
        result1 = new_full.select('title', 'publish_day')\
            .groupBy('publish_day')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy('publish_day', ascending=True)

        result1.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + "1.csv")

        self.end1 = time.time()

        self.start2 = time.time()
        result2 = new_full.select('title', 'publish_hour')\
            .groupBy('publish_hour')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy('publish_hour', ascending=True)

        result2.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + "2.csv")

        self.end2 = time.time()

        self.start3 = time.time()
        result3 = new_full.select('title', 'publish_day', 'publish_hour')\
            .groupBy('publish_day', 'publish_hour')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy('videos', ascending=False)

        result3.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + "3.csv")

        self.end3 = time.time()


if __name__ == "__main__":
    Job4()
