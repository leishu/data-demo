-- 查询限流窗口：60 秒滑动窗口内的每次请求一行。
CREATE TABLE IF NOT EXISTS lake_batch.query_events (client VARCHAR(80) NOT NULL, at_ms BIGINT NOT NULL);
