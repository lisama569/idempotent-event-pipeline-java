package com.github.lisama569.idempotenteventpipelinejava.pipeline.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.RejectionReason;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地文件的持久化实现。
 *
 * <p>状态整体快照写入 {@code <dir>/store.json}，采用“临时文件 + 原子改名”保证
 * 崩溃时不会出现半写文件；进程重启后从快照恢复，投递结论可预期。
 * 所有公共方法在内部锁上串行，保证并发处理下状态一致。
 */
@Component
public class FileEventStore implements EventStore {

    private final Object lock = new Object();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Path storeFile;
    private final Map<String, StoredEvent> events = new LinkedHashMap<>();
    private final Map<String, Long> lastDeliveredSeq = new ConcurrentHashMap<>();

    public FileEventStore(@Value("${pipeline.store.dir:./data/event-store}") String dir) {
        try {
            Path dirPath = Path.of(dir);
            Files.createDirectories(dirPath);
            this.storeFile = dirPath.resolve("store.json");
            load();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void load() throws IOException {
        if (!Files.exists(storeFile)) {
            return;
        }
        Snapshot snapshot = mapper.readValue(storeFile.toFile(), Snapshot.class);
        if (snapshot.events != null) {
            snapshot.events.forEach((k, v) -> events.put(k, v));
        }
        if (snapshot.lastDeliveredSeq != null) {
            lastDeliveredSeq.putAll(snapshot.lastDeliveredSeq);
        }
    }

    private void persistLocked() {
        try {
            Snapshot snapshot = new Snapshot();
            snapshot.events = new LinkedHashMap<>(events);
            snapshot.lastDeliveredSeq = new LinkedHashMap<>(lastDeliveredSeq);
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public StoredEvent saveIfAbsent(EventEnvelope envelope) {
        synchronized (lock) {
            StoredEvent existing = events.get(envelope.eventId());
            if (existing != null) {
                return existing;
            }
            StoredEvent created = new StoredEvent(envelope, EventStatus.PENDING, 0, null, 0L);
            events.put(envelope.eventId(), created);
            persistLocked();
            return created;
        }
    }

    @Override
    public Optional<StoredEvent> find(String eventId) {
        synchronized (lock) {
            return Optional.ofNullable(events.get(eventId));
        }
    }

    @Override
    public void updateStatus(String eventId, EventStatus status, int attempts,
                             RejectionReason reason, long nextAttemptAtMillis) {
        synchronized (lock) {
            StoredEvent current = events.get(eventId);
            if (current == null) {
                throw new IllegalArgumentException("unknown eventId: " + eventId);
            }
            events.put(eventId, new StoredEvent(current.envelope(), status, attempts,
                    reason, nextAttemptAtMillis));
            if (status == EventStatus.DELIVERED) {
                lastDeliveredSeq.merge(current.envelope().partitionKey(),
                        current.envelope().sequence(), Math::max);
            }
            persistLocked();
        }
    }

    @Override
    public long lastDeliveredSequence(String partitionKey) {
        return lastDeliveredSeq.getOrDefault(partitionKey, 0L);
    }

    @Override
    public List<StoredEvent> listDeadLetters() {
        synchronized (lock) {
            List<StoredEvent> dead = new ArrayList<>();
            for (StoredEvent e : events.values()) {
                if (e.status() == EventStatus.DEAD) {
                    dead.add(e);
                }
            }
            dead.sort(Comparator.comparing((StoredEvent e) -> e.envelope().partitionKey())
                    .thenComparingLong(e -> e.envelope().sequence()));
            return dead;
        }
    }

    /** 快照结构（Jackson 序列化用）。 */
    public static class Snapshot {
        public Map<String, StoredEvent> events;
        public Map<String, Long> lastDeliveredSeq;
    }
}
