from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, date_sub, count, max as spark_max, expr
from pyspark.sql.window import Window
import time

# 初始化 Spark
spark = SparkSession.builder \
    .appName("Day49_Continuous_Login") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 构造测试数据（与 MySQL 一致）
data = [
    (1, '2026-11-01'), (1, '2026-11-02'), (1, '2026-11-03'),
    (2, '2026-11-01'), (2, '2026-11-02'),
    (2, '2026-11-04'), (2, '2026-11-05'),
    (3, '2026-11-02'),
    (4, '2026-11-28'), (4, '2026-11-29'), (4, '2026-11-30'),
    (4, '2026-12-01'), (4, '2026-12-02'),
    (5, '2026-11-01'),
    (5, '2026-11-05'), (5, '2026-11-06'),
    (5, '2026-11-10')
]

# 创建 DataFrame
df = spark.createDataFrame(data, ["user_id", "login_date"]) \
          .withColumn("login_date", col("login_date").cast("date"))

# 1. 开窗打序号
window_spec = Window.partitionBy("user_id").orderBy("login_date")
df_with_rn = df.withColumn("rn", row_number().over(window_spec))

# 2. 日期减序号（核心技巧）
df_group = df_with_rn.withColumn("group_date", expr("date_sub(login_date, rn)"))
# 3. 按用户和基准日期分组，计算连续天数
df_continuous = df_group.groupBy("user_id", "group_date") \
                         .count() \
                         .withColumnRenamed("count", "continuous_days")

# 4. 取最大连续天数
df_result = df_continuous.groupBy("user_id") \
                           .agg(spark_max("continuous_days").alias("max_continuous_days")) \
                           .orderBy("user_id")

# 展示结果
df_result.show()

print("Day 49 任务计算完毕，休眠 300 秒，去对比 Spark UI...")
time.sleep(300)

spark.stop()
