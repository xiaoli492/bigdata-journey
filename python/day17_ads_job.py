from pyspark.sql import SparkSession

# 初始化 SparkSession，连接 Hive
spark = SparkSession.builder \
    .appName("Day17_ADS_Job") \
    .config("spark.sql.warehouse.dir", "hdfs://cat1:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# 开启动态分区（避免踩 Day16 的坑）
spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

# 如果 ads_tag_report 之前已经存在，先删掉，避免“路径已存在”的坑
spark.sql("DROP TABLE IF EXISTS ads_tag_report")

# 读取 DWS 层表 tag_counts，按日期汇总所有标签出现次数
print(">>> 正在计算每日标签总次数...")
ads_df = spark.sql("""
    SELECT dt, SUM(cnt) AS total_tags
    FROM tag_counts
    GROUP BY dt
""")

# 打印结果，确认数据没问题
ads_df.show()

# 写入 ADS 层新表 ads_tag_report
print(">>> 正在写入 ADS 层表 ads_tag_report...")
ads_df.write.mode("overwrite").saveAsTable("ads_tag_report")

print(">>> ADS 层写入完成！")
spark.stop()
