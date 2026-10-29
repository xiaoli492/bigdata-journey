import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark.implicits._

// 1. 模拟造数据：3 个类别，每个类别 5 个商品，带热度值
val data = Seq(
  ("A", "a1", 300),
  ("A", "a2", 800),
  ("A", "a3", 500),
  ("A", "a4", 200),
  ("A", "a5", 900),
  ("B", "b1", 400),
  ("B", "b2", 600),
  ("B", "b3", 100),
  ("B", "b4", 700),
  ("B", "b5", 350),
  ("C", "c1", 650),
  ("C", "c2", 150),
  ("C", "c3", 850),
  ("C", "c4", 450),
  ("C", "c5", 950)
)

val df = data.toDF("category", "item", "heat")
println("=== 原始数据 ===")
df.show(false)

// 2. 用 Window 窗口函数：按 category 分组，按 heat 降序，取每组前 3
val w = Window.partitionBy("category").orderBy(col("heat").desc)
val topN = df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3)

println("=== 每类 Top 3 ===")
topN.show(false)

// 3. 如果想写回 MySQL，把下面注释打开并改成你的密码
// topN.write.format("jdbc")
//   .option("url", "jdbc:mysql://localhost:3306/streaming_db?useSSL=false&characterEncoding=utf8")
//   .option("dbtable", "topn_result")
//   .option("user", "root")
//   .option("password", "Password123$")
//   .mode("overwrite")
//   .save()
// println("=== 已写回 MySQL topn_result ===")

