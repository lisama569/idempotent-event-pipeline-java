package com.github.lisama569.idempotenteventpipelinejava.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;

/**
 * 死信队列查询与重放。
 *
 * <p>重放规则：</p>
 * <ul>
 *   <li>仅按分区重放，且严格按原 seq 顺序逐个“放回队头”；</li>
 *   <li>重放把 DEAD_LETTER 复位为 RECEIVED、清退避并重置 attempt_count 为 0
 *       （显式重放给予全新重试预算），重试上限与退避规则仍然生效、判定确定；</li>
 *   <li>重放走的是同一条投递链路：幂等（eventId 不变、副作用键不变）
 *       与分区保序（仍然只在队头被处理）都不会被绕过；</li>
 *   <li>已 SUCCEEDED 的事件不在死信表中，重放永远不会让它们再产生副作用。</li>
 * </ul>
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final EventRepository repository;
    private final EventDispatcher dispatcher;

    public DeadLetterService(EventRepository repository, EventDispatcher dispatcher) {
        this.repository = repository;
        this.dispatcher = dispatcher;
    }

    public List<EventRecord> list(String partition) {
        return repository.findDeadLetters(partition);
    }

    /**
     * 重放某分区全部死信：按 seq 顺序复位，并当场在分区锁内串行推进，
     * 直到每个事件成功、再次死信，或进入退避等待（交由后台调度器到期继续）。
     * 返回被复位的事件数。
     */
    public int replay(String partition) {
        List<EventRecord> dead = repository.findDeadLetters(partition);
        int replayed = 0;
        for (EventRecord r : dead) {
            int rows = repository.replayDeadLetter(r.id, null);
            if (rows > 0) {
                replayed++;
                log.info("死信重放 eventId={} partition={} seq={} attempt={} 判定依据=管理员重放，按原序回到队头",
                        r.eventId, r.partition, r.seqInPartition, r.attemptCount);
                int guard = 0;
                // 该事件进入终态/退避后，继续排空分区里此前被它顶住的后续事件
                boolean advanced;
                do {
                    advanced = dispatcher.processPartitionOnce(r.partition) == 1;
                    EventRecord now = repository.findByEventId(r.eventId);
                    boolean selfWaiting = now != null && now.nextVisibleAt != null
                            && now.status != com.github.lisama569.idempotenteventpipelinejava.domain.EventStatus.SUCCEEDED
                            && now.status != com.github.lisama569.idempotenteventpipelinejava.domain.EventStatus.DEAD_LETTER;
                    if (selfWaiting) {
                        break; // 重放事件自身进入退避，后续事件继续被顶住，交后台到期处理
                    }
                } while (advanced && guard++ < 10_000);
            }
        }
        return replayed;
    }
}
