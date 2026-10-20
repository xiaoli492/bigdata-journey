-- Day 13: Hive 复杂类型函数 EXPLODE 与 COLLECT_SET

-- 1. 测试 EXPLODE：数组拆成多行
SELECT explode(array('热血', '奇幻', '战斗')) AS tag;

-- 2. LATERAL VIEW + EXPLODE：一行变多行
SELECT a.title, tag
FROM anime_rank a
LATERAL VIEW explode(array('热血', '奇幻', '战斗')) t AS tag
LIMIT 20;

-- 3. COLLECT_SET：多行合并成数组，行转列
SELECT collect_set(title) FROM anime_rank;
