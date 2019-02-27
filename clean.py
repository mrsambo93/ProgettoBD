from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


# Best day and time to publish video by country
class Cleaner:

    PATH = "./youtube-new/"
    OUTPUT_FOLDER = './output/clean/'
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
        columns = new_full.schema.names
        for column in columns:
            result = new_full.withColumn(column, sf.regexp_replace(sf.col(column), "[\\r\\n]", " "))

        result.coalesce(1).write.format('csv') \
            .option('header', 'true') \
            .mode('overwrite') \
            .csv(self.OUTPUT_FOLDER + country + ".csv")


if __name__ == "__main__":
    Cleaner()
