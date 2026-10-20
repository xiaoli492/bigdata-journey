-- day12_hive_partition.sql

-- 允许动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 如果存在旧表，先删掉，避免重复跑报错
DROP TABLE IF EXISTS anime_partitioned;

-- 创建分区表
-- hot_level 是分区字段，不在普通字段列表里
CREATE TABLE anime_partitioned (
    title    STRING,
    link     STRING,
    img_url  STRING,
    hot_value INT,
    sub_title STRING
)
PARTITIONED BY (hot_level STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

-- 动态插入数据
-- 把 anime_rank 的数据根据 hot_value 拆成 high / medium / low
INSERT INTO TABLE anime_partitioned PARTITION (hot_level)
SELECT
    title,
    link,
    img_url,
    -- hot_value 是 "🔥846" 这种格式，先去掉非数字，再转 INT
    CAST(REGEXP_REPLACE(hot_value, '[^0-9]', '') AS INT) AS hot_value,
    sub_title,
    -- 根据热度生成分区值
    CASE
        WHEN CAST(REGEXP_REPLACE(hot_value, '[^0-9]', '') AS INT) >= 700 THEN 'high'
        WHEN CAST(REGEXP_REPLACE(hot_value, '[^0-9]', '') AS INT) >= 500 THEN 'medium'
        ELSE 'low'
    END AS hot_level
FROM anime_rank;