from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, lit

spark = SparkSession.builder \
    .appName("Day14_SparkIntro") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 表
df = spark.sql("SELECT * FROM anime_rank")
df.show(10, truncate=False)

# 标签炸裂与聚合（已修复字面量问题）
tag_df = df.select("title", explode(array(lit("热血"), lit("奇幻"), lit("战斗"))).alias("tag"))
tag_df.groupBy("tag").count().orderBy("count", ascending=False).show()

spark.stop()
