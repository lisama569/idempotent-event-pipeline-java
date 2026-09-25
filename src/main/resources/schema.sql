-- 本地事件流水线持久化表（H2 嵌入式文件库）

CREATE TABLE IF NOT EXISTS events (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id         VARCHAR(128) NOT NULL,
    partition_key    VARCHAR(128) NOT NULL,
    seq_in_partition BIGINT       NOT NULL,
    version          INT          NOT NULL,
    type             VARCHAR(128) NOT NULL,
    payload_json     CLOB,
    priority         INT          NOT NULL DEFAULT 0,
    status           VARCHAR(32)  NOT NULL,
    attempt_count    INT          NOT NULL DEFAULT 0,
    next_visible_at  TIMESTAMP,
    locked_by        VARCHAR(64),
    locked_at        TIMESTAMP,
    reject_reason    VARCHAR(64),
    reject_detail    VARCHAR(1024),
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_events_event_id UNIQUE (event_id),
    CONSTRAINT uk_events_partition_seq UNIQUE (partition_key, seq_in_partition)
);

CREATE INDEX IF NOT EXISTS idx_events_dispatch
    ON events (partition_key, status, next_visible_at, seq_in_partition);

CREATE TABLE IF NOT EXISTS partition_seq (
    partition_key VARCHAR(128) PRIMARY KEY,
    last_seq      BIGINT NOT NULL
);
