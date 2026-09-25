package com.github.lisama569.idempotenteventpipelinejava.repo;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.github.lisama569.idempotenteventpipelinejava.domain.EventStatus;
import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;
import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;

/**
 * 基于 H2（JDBC）的事件存储。
 *
 * <p>关键并发控制：</p>
 * <ul>
 *   <li>events.event_id 唯一约束兜底接收去重；</li>
 *   <li>(partition_key, seq_in_partition) 唯一约束兜底序号；</li>
 *   <li>分区序号在事务内对 partition_seq 行做条件更新分配；</li>
 *   <li>投递占用使用 CAS：UPDATE ... WHERE id=? AND locked_by IS NULL，
 *       stale 锁可被回收（进程崩溃恢复）。</li>
 * </ul>
 */
@Repository
public class JdbcEventRepository implements EventRepository {

    private final JdbcTemplate jdbc;

    public JdbcEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<EventRecord> MAPPER = (rs, n) -> {
        EventRecord r = new EventRecord();
        r.id = rs.getLong("id");
        r.eventId = rs.getString("event_id");
        r.partition = rs.getString("partition_key");
        r.seqInPartition = rs.getLong("seq_in_partition");
        r.version = rs.getInt("version");
        r.type = rs.getString("type");
        r.payloadJson = rs.getString("payload_json");
        r.priority = rs.getInt("priority");
        r.status = EventStatus.valueOf(rs.getString("status"));
        r.attemptCount = rs.getInt("attempt_count");
        Timestamp nva = rs.getTimestamp("next_visible_at");
        r.nextVisibleAt = nva == null ? null : nva.toInstant();
        r.lockedBy = rs.getString("locked_by");
        Timestamp la = rs.getTimestamp("locked_at");
        r.lockedAt = la == null ? null : la.toInstant();
        String rr = rs.getString("reject_reason");
        r.rejectReason = rr == null ? RejectReason.NONE : RejectReason.valueOf(rr);
        r.rejectDetail = rs.getString("reject_detail");
        r.createdAt = rs.getTimestamp("created_at").toInstant();
        r.updatedAt = rs.getTimestamp("updated_at").toInstant();
        return r;
    };

    @Override
    public boolean existsByEventId(String eventId) {
        Integer cnt = jdbc.queryForObject(
                "SELECT COUNT(1) FROM events WHERE event_id = ?", Integer.class, eventId);
        return cnt != null && cnt > 0;
    }

    @Override
    @Transactional
    public EventRecord insertIfAbsent(EventRecord record) {
        EventRecord existing = findByEventId(record.eventId);
        if (existing != null) {
            return existing;
        }
        long seq = nextSequence(record.partition);
        try {
            jdbc.update("""
                    INSERT INTO events
                      (event_id, partition_key, seq_in_partition, version, type, payload_json,
                       priority, status, attempt_count, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    record.eventId, record.partition, seq, record.version, record.type,
                    record.payloadJson, record.priority, EventStatus.RECEIVED.name(), 0);
        } catch (DuplicateKeyException e) {
            return findByEventId(record.eventId);
        }
        return findByEventId(record.eventId);
    }

    @Override
    public EventRecord findByEventId(String eventId) {
        List<EventRecord> list = jdbc.query(
                "SELECT * FROM events WHERE event_id = ?", MAPPER, eventId);
        return list.isEmpty() ? null : list.get(0);
    }

    @Override
    @Transactional
    public long nextSequence(String partition) {
        int updated = jdbc.update(
                "UPDATE partition_seq SET last_seq = last_seq + 1 WHERE partition_key = ?", partition);
        if (updated == 0) {
            jdbc.update("INSERT INTO partition_seq (partition_key, last_seq) VALUES (?, 1)", partition);
            return 1;
        }
        Long last = jdbc.queryForObject(
                "SELECT last_seq FROM partition_seq WHERE partition_key = ?", Long.class, partition);
        return last == null ? 1 : last;
    }

    @Override
    public List<String> findActivePartitions() {
        return jdbc.queryForList("""
                SELECT DISTINCT partition_key FROM events
                 WHERE status IN ('RECEIVED','IN_FLIGHT','RETRYING')
                """, String.class);
    }

    @Override
    public EventRecord findHeadForDispatch(String partition) {
        // 分区内按 seq 保序：取未终态、退避已到期 的最小 seq 行；
        // 注意调用方必须先 CAS 占用成功，IN_FLIGHT 的行会被 stale 锁回收机制重新拾取。
        List<EventRecord> list = jdbc.query("""
                SELECT * FROM events
                 WHERE partition_key = ?
                   AND status IN ('RECEIVED','IN_FLIGHT','RETRYING')
                   AND (next_visible_at IS NULL OR next_visible_at <= CURRENT_TIMESTAMP)
                 ORDER BY seq_in_partition
                 LIMIT 1
                """, MAPPER, partition);
        return list.isEmpty() ? null : list.get(0);
    }

    @Override
    public boolean tryAcquire(EventRecord record, String workerToken, Instant staleBefore) {
        int rows = jdbc.update("""
                UPDATE events
                   SET locked_by = ?, locked_at = CURRENT_TIMESTAMP,
                       status = 'IN_FLIGHT', updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                   AND (locked_by IS NULL OR locked_by = ? OR locked_at < ?)
                """,
                workerToken, record.id, workerToken, Timestamp.from(staleBefore));
        return rows > 0;
    }

    @Override
    public void applySuccess(EventRecord record, String workerToken) {
        jdbc.update("""
                UPDATE events
                   SET status = 'SUCCEEDED', attempt_count = attempt_count + 1,
                       locked_by = NULL, locked_at = NULL, next_visible_at = NULL,
                       reject_reason = NULL, reject_detail = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND locked_by = ?
                """, record.id, workerToken);
    }

    @Override
    public void applyRetry(EventRecord record, String workerToken, Instant nextVisibleAt) {
        jdbc.update("""
                UPDATE events
                   SET status = 'RETRYING', attempt_count = attempt_count + 1,
                       next_visible_at = ?, locked_by = NULL, locked_at = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND locked_by = ?
                """, Timestamp.from(nextVisibleAt), record.id, workerToken);
    }

    @Override
    public void applyDeadLetter(EventRecord record, String workerToken,
                                RejectReason reason, String detail) {
        jdbc.update("""
                UPDATE events
                   SET status = 'DEAD_LETTER', attempt_count = attempt_count + 1,
                       locked_by = NULL, locked_at = NULL,
                       reject_reason = ?, reject_detail = ?,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND locked_by = ?
                """, reason.name(), truncate(detail), record.id, workerToken);
    }

    @Override
    public List<EventRecord> findDeadLetters(String partition) {
        if (partition == null || partition.isBlank()) {
            return jdbc.query(
                    "SELECT * FROM events WHERE status = 'DEAD_LETTER' "
                            + "ORDER BY partition_key, seq_in_partition", MAPPER);
        }
        return jdbc.query(
                "SELECT * FROM events WHERE status = 'DEAD_LETTER' AND partition_key = ? "
                        + "ORDER BY seq_in_partition", MAPPER, partition);
    }

    @Override
    public int replayDeadLetter(long id, String workerToken) {
        // 显式重放：按原 seq 回到队头，给予全新重试预算（attempt_count=0），退避与拒绝原因清零；
        // event_id 不变（幂等键不变），seq 不变（保序不变），成功过的事件永远不会走到这里。
        return jdbc.update("""
                UPDATE events
                   SET status = 'RECEIVED', attempt_count = 0, next_visible_at = NULL,
                       locked_by = NULL, locked_at = NULL,
                       reject_reason = NULL, reject_detail = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = ? AND status = 'DEAD_LETTER'
                """, id);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 1000 ? s : s.substring(0, 1000);
    }
}
