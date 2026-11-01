#!/bin/bash
# 大数据集群日志清理脚本
# 清理 Hadoop 日志目录下 7 天前的 .log 文件

LOG_DIR="/usr/local/src/hadoop/logs"

echo "开始清理 $LOG_DIR 下 7 天前的 .log 文件..."
find "$LOG_DIR" -type f -name "*.log" -mtime +7 -print -delete

echo "清理完成。"

echo "清空 HDFS 回收站..."
hdfs dfs -expunge

echo "HDFS 回收站清空完成。"
