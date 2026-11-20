from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, length, count

spark = SparkSession.builder \
    .appName("Day50_CleanJob") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = [
    (1, "Python"), (2, " python "), (3, "PYTHON"),
    (4, "Spark"),  (5, "spark"),   (6, ""),
    (7, "Flink"),  (8, "flink"),   (9, "  Flink  "),
    (10, "Python"),
]

df_raw = spark.createDataFrame(raw, ["user_id", "tag"])

df_clean = df_raw \
    .withColumn("tag", lower(trim(col("tag")))) \
    .filter(length(col("tag")) > 0)

df_stat = df_clean.groupBy("tag").agg(count("*").alias("cnt")).orderBy(col("cnt").desc())

df_stat.coalesce(1).write.mode("overwrite").csv("file:///root/day50_etl/output/tag_stat", header=True)

df_stat.show()
spark.stop()
