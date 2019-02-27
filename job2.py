from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as t
import json


# Best categories by country
class Job2:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/job2/'
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

        category_id = dict()
        with open(self.PATH + "US_category_id.json") as f:
            categories = json.load(f)
            for category in categories['items']:
                category_id[int(category['id'])] = category['snippet']['title']

        mapping_expr = sf.udf(lambda x: category_id[x], t.StringType())

        new_full = full.withColumn('category_id', mapping_expr(sf.col('category_id')))

        result = new_full.select('category_id')\
            .groupBy('category_id')\
            .agg(sf.count('category_id').alias('videos'))\
            .orderBy('videos', ascending=False)

        result.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + ".csv")


if __name__ == "__main__":
    Job2()
