import org.apache.spark.sql.functions._

// 1. 准备好 JDBC 连接参数（密码换成你真实的 MySQL 密码）
val jdbcUrl = "jdbc:mysql://localhost:3306/streaming_db?useSSL=false&characterEncoding=utf8"
val userName = "root"
val userPass = "Password123$"

// 2. 从 MySQL 读取昨天写好的 word_counts 测试表
val sourceDF = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "word_counts")
  .option("user", userName)
  .option("password", userPass)
  .load()

sourceDF.show(false)

// 3. 做一次简单聚合：每个单词的总次数
val result = sourceDF
  .groupBy("word")
  .sum("cnt")
  .toDF("word", "total_cnt")

result.show(false)

// 4. 写回 MySQL 的另一张表 word_counts_scala
result.write.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "word_counts_scala")
  .option("user", userName)
  .option("password", userPass)
  .mode("overwrite")
  .save()

println("=== 写入 MySQL 成功！去查 word_counts_scala 吧 ===")
