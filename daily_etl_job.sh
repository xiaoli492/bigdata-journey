#!/bin/bash

# ================= 配置区域 =================
SCRIPT_DIR="/root/day50_etl"
PYSPARK_JOB="${SCRIPT_DIR}/clean_job.py"
LOG_FILE="${SCRIPT_DIR}/etl.log"
FLAG_FILE="${SCRIPT_DIR}/.etl_success_flag_$(date +%Y-%m-%d)"
MAX_RETRY=3
# ===========================================

# 日志函数
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a ${LOG_FILE}
}

# 1. 检查今天是否已经跑过任务
if [ -f "${FLAG_FILE}" ]; then
    log "任务今日已执行成功，直接退出。"
    exit 0
fi

log "========== ETL 任务开始 =========="

# 2. 执行 PySpark 任务（带失败重试机制）
retry_count=0
while [ ${retry_count} -lt ${MAX_RETRY} ]; do
    log "正在执行 PySpark 清洗任务... (第 $((retry_count + 1)) 次尝试)"
    
    # 执行任务
    spark-submit ${PYSPARK_JOB}
    
    # 判断执行结果
    if [ $? -eq 0 ]; then
        log "PySpark 任务执行成功！"
        
        # 3. 执行 Spark SQL 统计 (模拟)
        log "正在执行 Spark SQL 统计任务..."
        # 这里模拟执行 Spark SQL，实际可替换为 spark-sql -e "xxx"
        sleep 2
        log "Spark SQL 统计任务执行成功！"
        
        # 4. 任务全部成功，打上成功标记
        touch ${FLAG_FILE}
        log "任务执行成功！"
        log "========== ETL 任务结束 =========="
        exit 0
    else
        retry_count=$((retry_count + 1))
        log "PySpark 任务失败，准备重试..."
        sleep 3
    fi
done

# 5. 3次重试均失败，触发告警
log "ERROR: PySpark 任务重试 ${MAX_RETRY} 次后依然失败！"
echo "【模拟告警】ETL 任务失败，请检查服务器日志！" 
log "========== ETL 任务异常结束 =========="
exit 1
