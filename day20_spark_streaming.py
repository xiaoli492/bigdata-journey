from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

# 创建 SparkSession
spark = SparkSession.builder.appName("KafkaWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # 屏蔽多余心跳日志

# 读取 Kafka（使用括号隐式续行，不需要反斜杠）
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.5:9092,192.168.1.6:9092,192.168.1.7:9092") \
    .option("subscribe", "cluster_test") \
    .option("startingOffsets", "earliest") \
    .load()

# 处理数据（词频统计）
lines = df.selectExpr("CAST(value AS STRING) as message")
words = lines.select(explode(split(col("message"), " ")).alias("word"))
word_counts = words.groupBy("word").count()

# 输出到控制台
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_v3") \
    .start()

query.awaitTermination()
