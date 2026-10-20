from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, lit, col, count

# 1. 初始化 SparkSession，并连接 Hive
spark = SparkSession.builder \
    .appName("Day15_ETL") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. 读取 Hive 表 anime_rank
df = spark.sql("SELECT * FROM anime_rank")
df.show(10, truncate=False)

# 3. 用 withColumn + explode 把标签数组炸开
# 注意：如果 anime_rank 里真的有 tags 列，可以改成 explode(col("tags"))
tag_df = df.withColumn("tag", explode(array(lit("热血"), lit("奇幻"), lit("战斗"))))
tag_df = tag_df.select("title", "tag")

# 4. 过滤空标签（正式场景很有用，这里不丢失数据）
tag_df = tag_df.filter(col("tag").isNotNull())

# 5. 按 tag 分组统计数量，字段重命名为 cnt
tag_counts = tag_df.groupBy("tag").agg(count("*").alias("cnt"))
tag_counts.show()

# 6. 写入 Hive 新表 tag_counts
tag_counts.write.mode("overwrite").saveAsTable("tag_counts")

spark.stop()
