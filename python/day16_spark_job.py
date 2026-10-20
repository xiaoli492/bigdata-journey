from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, lit, col, current_date

# ================= 配置变量 =================
INPUT_TABLE = "anime_rank"
OUTPUT_TABLE = "tag_counts"
PARTITION_COL = "dt"  # 按日期分区

# ================= 初始化 Spark =================
print(">>> 正在初始化 SparkSession...")
spark = SparkSession.builder \
    .appName("Day16_ETL_Engineered") \
    .config("spark.sql.warehouse.dir", "hdfs://cat1:9000/user/hive/warehouse") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# ================= 核心逻辑封装 =================
def read_data(spark, table_name):
    """读取 Hive 表数据"""
    print(f">>> 开始读取数据: [{table_name}]...")
    df = spark.sql(f"SELECT * FROM {table_name}")
    return df

def process_data(df):
    """处理数据：炸开标签、过滤空值、聚合统计、添加日期分区列"""
    print(">>> 正在聚合标签与处理数据...")
    # 1. 标签炸开
    tag_df = df.withColumn("tag", explode(array(lit("热血"), lit("奇幻"), lit("战斗"))))
    # 2. 过滤空标签
    tag_df = tag_df.filter(col("tag").isNotNull())
    # 3. 按标签分组统计，重命名为 cnt
    result_df = tag_df.groupBy("tag").agg({"*": "count"}).withColumnRenamed("count(1)", "cnt")
    # 4. 添加当前日期作为分区列
    result_df = result_df.withColumn(PARTITION_COL, current_date())
    return result_df

def write_data(df, table_name, partition_col):
    """写入 Hive 分区表"""
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")    
    print(f">>> 正在写入 Hive 分区表: [{table_name}] (按 {partition_col} 分区覆盖)...")
    df.write \
      .mode("overwrite") \
      .partitionBy(partition_col) \
      .format("hive") \
      .saveAsTable(table_name)
    print(">>> 数据写入完成！")

# ================= 主程序执行 =================
try:
    raw_df = read_data(spark, INPUT_TABLE)
    raw_df.show(5, truncate=False)
    
    processed_df = process_data(raw_df)
    processed_df.show()
    
    write_data(processed_df, OUTPUT_TABLE, PARTITION_COL)
finally:
    spark.stop()
    print(">>> SparkSession 已停止，任务结束。")
