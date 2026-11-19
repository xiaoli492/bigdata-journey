-- Day 49: 业务实战 - 计算用户连续登录天数

-- 1. 造数据
CREATE DATABASE IF NOT EXISTS day49_demo;
USE day49_demo;

CREATE TABLE IF NOT EXISTS user_login (
    user_id     INT         NOT NULL,
    login_date  DATE        NOT NULL,
    PRIMARY KEY (user_id, login_date)
);

INSERT INTO user_login (user_id, login_date) VALUES
(1, '2026-11-01'), (1, '2026-11-02'), (1, '2026-11-03'),
(2, '2026-11-01'), (2, '2026-11-02'),
(2, '2026-11-04'), (2, '2026-11-05'),
(3, '2026-11-02'),
(4, '2026-11-28'), (4, '2026-11-29'), (4, '2026-11-30'),
(4, '2026-12-01'), (4, '2026-12-02'),
(5, '2026-11-01'),
(5, '2026-11-05'), (5, '2026-11-06'),
(5, '2026-11-10');

-- 2. 普通 SQL 实现连续登录天数计算（日期减序号技巧）
SELECT 
    user_id,
    MAX(continuous_days) AS max_continuous_days
FROM (
    SELECT 
        user_id,
        group_date,
        COUNT(*) AS continuous_days
    FROM (
        SELECT 
            user_id,
            DATE_SUB(login_date, INTERVAL rn DAY) AS group_date
        FROM (
            SELECT 
                user_id, 
                login_date,
                ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY login_date) AS rn
            FROM user_login
        ) t1
    ) t2
    GROUP BY user_id, group_date
) t3
GROUP BY user_id;
