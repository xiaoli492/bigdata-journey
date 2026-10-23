from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

# 1. 创建 SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamingToMySQL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. 读取 Kafka 流数据
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.5:9092,192.168.1.6:9092,192.168.1.7:9092") \
    .option("subscribe", "cluster_test") \
    .option("startingOffsets", "latest") \
    .load()

# 3. 转换数据：二进制 -> 字符串 -> 切分单词
lines = df.selectExpr("CAST(value AS STRING) as message")
words = lines.select(explode(split(col("message"), " ")).alias("word"))
word_counts = words.groupBy("word").count().toDF("word", "cnt")

# 4. 定义 foreachBatch 写入函数：每批数据写入 MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/streaming_db?useSSL=false&characterEncoding=utf8") \
        .option("dbtable", "word_counts") \
        .option("user", "root") \
        .option("password", "Password123$") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written to MySQL")

# 5. 写出到 MySQL
query = word_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "/tmp/spark_checkpoint_mysql") \
    .start()

query.awaitTermination()
