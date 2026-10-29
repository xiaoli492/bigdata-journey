
## Day 28 笔记：数据倾斜的三大原因及解决思路

1. Key 分布不均匀 -> 加盐打散：给 key 加随机前缀，让数据分散到不同分区。
2. 单个 key 数据量过大 -> 提高并行度：调整 spark.sql.shuffle.partitions，或使用 repartition/coalesce。
3. 大表 join 小表导致数据倾斜 -> 小表广播：使用 broadcast join，避免大量 shuffle。
