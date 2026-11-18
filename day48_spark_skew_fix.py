from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, rand, expr
import time

spark = SparkSession.builder \
    .appName("SkewFixAggregation") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 强制读本地文件
df = spark.read.csv("file:///root/xiao/skew_data.txt", schema="key STRING, value STRING")

# 1. 加盐打散：给 key 加上 0-9 的随机前缀
df_salt = df.withColumn("salt_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))

# 2. 局部聚合（带前缀）
df_local_agg = df_salt.groupBy("salt_key").count()

# 3. 去掉前缀，全局聚合
# 用 expr 直接写 SQL 的 substring，截取盐值前的原 key
df_global_agg = df_local_agg \
    .withColumn("original_key", expr("substring(salt_key, 1, length(salt_key) - 2)")) \
    .groupBy("original_key") \
    .sum("count")

# 展示结果
df_global_agg.show(10)

print("优化任务计算完毕，休眠 300 秒，去对比 Spark UI...")
time.sleep(300)

spark.stop()
