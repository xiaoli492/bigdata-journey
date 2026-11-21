from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day50_Clean_Job").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = [("python", 4), ("flink", 3), ("spark", 2)]
df = spark.createDataFrame(data, ["tag", "cnt"])

df.coalesce(1).write.mode("overwrite").csv("file:///root/day50_etl/output/tag_stat", header=True)

spark.stop()
