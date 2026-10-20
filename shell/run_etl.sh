#!/bin/bash
set -e

cd /root/xiao

echo "1. 开始运行 DWD 层任务..."
spark-submit --master local[*] day16_spark_job.py

echo "2. 开始运行 ADS 层任务..."
spark-submit --master local[*] day17_ads_job.py

echo "任务全部完成！"
