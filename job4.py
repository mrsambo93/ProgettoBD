from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


# Best day and time to publish video by country
class Job4:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/job4/'
    COUNTRIES = ["CA", "DE", "FR", "GB", "US"]

    def __init__(self):

        spark = SparkSession.builder.master('local').appName('progettoBD').getOrCreate()

        for country in self.COUNTRIES:
            self.load_data(spark, country)

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

        result1 = new_full.select('title', 'publish_day')\
            .groupBy('publish_day')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy('publish_day', ascending=True)

        result2 = new_full.select('title', 'publish_hour')\
            .groupBy('publish_hour')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy('publish_hour', ascending=True)

        result3 = new_full.select('title', 'publish_day', 'publish_hour')\
            .groupBy('publish_day', 'publish_hour')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy('videos', ascending=False)

        result1.coalesce(1).write.format('csv')\
            .option('header', 'true')\
            .mode('overwrite')\
            .csv(self.OUTPUT_FOLDER + country + "1.csv")

        result2.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + "2.csv")

        result3.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + "3.csv")


if __name__ == "__main__":
    Job4()
