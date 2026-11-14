-- Day 44: Flink SQL datagen 流式聚合实战
SET sql-client.execution.result-mode=tableau;

-- 1. 创建数据源表（使用 datagen 连接器模拟无界流数据）
CREATE TABLE source_table (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.behavior.length' = '5'
);

-- 2. 创建下沉表（打印到控制台）
CREATE TABLE sink_table (
    behavior STRING,
    real_count BIGINT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- 3. 执行流式聚合查询（按行为分组，统计每 10 秒的窗口计数）
INSERT INTO sink_table
SELECT 
    behavior,
    COUNT(*) AS real_count,
    TUMBLE_START(ts, INTERVAL '10' SECOND) AS window_start,
    TUMBLE_END(ts, INTERVAL '10' SECOND) AS window_end
FROM source_table
GROUP BY 
    behavior, 
    TUMBLE(ts, INTERVAL '10' SECOND);
