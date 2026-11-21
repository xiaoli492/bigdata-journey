# -*- coding: utf-8 -*-
"""
Day 51: 数据质量监控模块
校验规则：
1. 总行数不能为 0（为 0 直接抛异常退出）
2. cnt 总和环比不能下降超过 50%
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# ============ 配置 ============
DATA_PATH = "file:///root/day50_etl/output/tag_stat"
BASELINE_FILE = "/root/day51_quality/baseline_cnt.txt"
MAX_DROP_RATE = 0.5  # 最大允许下降 50%
# ==============================

def log(msg):
    print(f"[QualityCheck] {msg}")

def read_baseline():
    """读取昨天的 cnt 总和基准值"""
    try:
        with open(BASELINE_FILE, "r") as f:
            value = float(f.read().strip())
        log(f"读取基准值成功：昨日 cnt 总和 = {value}")
        return value
    except Exception as e:
        log(f"读取基准文件失败：{e}")
        return None

def main():
    spark = SparkSession.builder \
        .appName("Day51_DataQualityCheck") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log("========== 数据质量检查开始 ==========")

    # 1. 读取 tag 结果表
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
    df.createOrReplaceTempView("tag_stat")

    # ===== 校验 1：总行数不能为 0 =====
    total_rows = df.count()
    log(f"当前结果表总行数 = {total_rows}")
    if total_rows == 0:
        log("[FAIL] 校验1失败：结果表总行数为 0，数据为空！")
        spark.stop()
        sys.exit(1)
    log("[PASS] 校验1通过：总行数 > 0")

    # ===== 校验 2：cnt 总和环比下降不能超过 50% =====
    baseline = read_baseline()
    if baseline is not None:
        today_cnt = df.agg(spark_sum(col("cnt"))).collect()[0][0] or 0
        log(f"今日 cnt 总和 = {today_cnt}")

        if baseline == 0:
            drop_rate = 0.0 if today_cnt == 0 else float("inf")
        else:
            drop_rate = (baseline - today_cnt) / baseline

        log(f"环比下降率 = {drop_rate:.2%}")
        if drop_rate > MAX_DROP_RATE:
            log(f"[FAIL] 校验2失败：cnt 总和环比下降 {drop_rate:.2%}，超过 {MAX_DROP_RATE:.0%} 阈值！")
            spark.stop()
            sys.exit(1)
        log(f"[PASS] 校验2通过：环比下降 {drop_rate:.2%}，未超阈值")
    else:
        log("[WARN] 无基准值，跳过校验2")

    log("========== 数据质量检查全部通过 ==========")
    spark.stop()
    sys.exit(0)

if __name__ == "__main__":
    main()
