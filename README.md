
## Day 28 笔记：数据倾斜的三大原因及解决思路

1. Key 分布不均匀 -> 加盐打散：给 key 加随机前缀，让数据分散到不同分区。
2. 单个 key 数据量过大 -> 提高并行度：调整 spark.sql.shuffle.partitions，或使用 repartition/coalesce。
3. 大表 join 小表导致数据倾斜 -> 小表广播：使用 broadcast join，避免大量 shuffle。

## Day 29 笔记：Spark on YARN

1. Spark on YARN 有两种模式：
   - client 模式：Driver 运行在提交任务的客户端，便于看日志。
   - cluster 模式：Driver 运行在 YARN 的 ApplicationMaster 中，适合生产环境。

2. YARN 资源分配：
   - ResourceManager 负责全局资源管理和调度。
   - NodeManager 负责单个节点上的 Container 启动与监控。
   - 每个 Spark Executor 会以 Container 形式运行。

3. 遇到 YARN 内存不足时，可以调整：
   - spark.executor.memory
   - spark.executor.cores
   - spark.yarn.executor.memoryOverhead

4. 数据倾斜解决思路：加盐打散、提高并行度、小表广播。
