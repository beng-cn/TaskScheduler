-- TaskScheduler 数据库初始化脚本
-- 使用方式: mysql -u root -p < store/init.sql
-- 或在程序中通过 MySQL 连接自动建表

CREATE DATABASE IF NOT EXISTS task_scheduler
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE task_scheduler;

-- 任务表：存储所有任务的完整生命周期数据
CREATE TABLE IF NOT EXISTS tasks (
    id             VARCHAR(64)   NOT NULL PRIMARY KEY COMMENT '任务唯一标识',
    name           VARCHAR(255)  NOT NULL             COMMENT '任务名称',
    type           VARCHAR(50)   NOT NULL             COMMENT '任务类型（http_call/data_clean/flash_warmup）',
    payload        TEXT                               COMMENT '任务负载（JSON字符串）',
    status         VARCHAR(20)   NOT NULL DEFAULT 'pending'
                   COMMENT '任务状态: pending/running/done/failed/timeout/retrying',
    priority       INT           NOT NULL DEFAULT 0   COMMENT '优先级（越大越优先）',
    retries        INT           NOT NULL DEFAULT 0   COMMENT '已重试次数',
    max_retries    INT           NOT NULL DEFAULT 3   COMMENT '最大重试次数',
    timeout        BIGINT        NOT NULL DEFAULT 30  COMMENT '超时时间（秒）',
    max_latency_ms BIGINT        NOT NULL DEFAULT 0   COMMENT '响应延迟阈值（毫秒），超过告警但不算失败',
    repeat_sec     BIGINT        NOT NULL DEFAULT 0   COMMENT '循环间隔（秒），0表示不循环',
    scheduled_at   DATETIME      NULL                 COMMENT '计划执行时间',
    started_at     DATETIME      NULL                 COMMENT '实际开始执行时间',
    finished_at    DATETIME      NULL                 COMMENT '完成时间',
    result         TEXT                               COMMENT '执行结果（成功时）',
    error          TEXT                               COMMENT '错误信息（失败时）',
    steps          TEXT                               COMMENT '子步骤详情（JSON数组，仅多步runner填充）',
    created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',

    INDEX idx_status (status),
    INDEX idx_type (type),
    INDEX idx_scheduled (status, scheduled_at),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务调度表';

-- 分布式锁表：用于多节点部署时防止重复调度
CREATE TABLE IF NOT EXISTS locks (
    lock_key    VARCHAR(128) NOT NULL PRIMARY KEY COMMENT '锁的键名',
    expiry      BIGINT       NOT NULL             COMMENT '过期时间（Unix毫秒时间戳）',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='分布式锁表';
