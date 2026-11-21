#!/bin/bash

# ================= 配置区域 =================
SCRIPT_DIR="/root/day50_etl"
PYSPARK_JOB="/root/day50_etl/clean_job.py"
QUALITY_CHECK_JOB="/root/day51_quality/data_quality_check.py"
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
    
    spark-submit ${PYSPARK_JOB}
    
    if [ $? -eq 0 ]; then
        log "PySpark 任务执行成功！"
        
        log "正在执行 Spark SQL 统计任务..."
        sleep 2
        log "Spark SQL 统计任务执行成功！"
        
        # ===== 数据质量检查 =====
        log "正在执行数据质量检查..."
        spark-submit ${QUALITY_CHECK_JOB}
        
        if [ $? -ne 0 ]; then
            echo -e "\033[31m[WARNING] 数据质量异常！请立即排查！\033[0m"
            log "数据质量检查失败，任务中断！"
            exit 1
        fi
        log "数据质量检查通过！"

        # 3. 任务全部成功，打上成功标记
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

log "ERROR: PySpark 任务重试 ${MAX_RETRY} 次后依然失败！"
echo -e "\033[31m[WARNING] ETL 任务失败，请检查服务器日志！\033[0m"
log "========== ETL 任务异常结束 =========="
exit 1
