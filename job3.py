from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


# Days to trend by country
class Job3:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/job3/'
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

        new_full = full.withColumn('trending_date', sf.to_timestamp(sf.col('trending_date'), format='yy.dd.MM'))

        result = new_full.withColumn('days', sf.datediff(sf.col('trending_date'), sf.col('publish_time')))\
            .select('title', 'days')\
            .groupBy('days')\
            .agg(sf.count('title').alias('videos'))\
            .orderBy("days", ascending=True)

        result.coalesce(1).write.format('csv')\
            .option('header', 'true')\
            .mode('overwrite')\
            .csv(self.OUTPUT_FOLDER + country + ".csv")


if __name__ == "__main__":
    Job3()
