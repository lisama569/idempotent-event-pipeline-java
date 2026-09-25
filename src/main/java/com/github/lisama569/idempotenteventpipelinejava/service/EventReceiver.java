package com.github.lisama569.idempotenteventpipelinejava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.lisama569.idempotenteventpipelinejava.model.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.model.ReceiveOutcome;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;

/**
 * 事件接收入口。
 *
 * <p>判定类别：</p>
 * <ul>
 *   <li>ACCEPTED：校验通过且首次落库；</li>
 *   <li>DUPLICATE_EVENT：eventId 已存在（重复投递，返回既有记录，不新增序号、不产生副作用）；</li>
 *   <li>其余拒绝原因由 {@link EventValidator} 在解析阶段以 {@code ValidationException} 抛出。</li>
 * </ul>
 * 判定是确定性的：同一输入（在库状态不变时）多次提交，结论一致。
 */
@Service
public class EventReceiver {

    private static final Logger log = LoggerFactory.getLogger(EventReceiver.class);

    private final EventValidator validator;
    private final EventRepository repository;

    public EventReceiver(EventValidator validator, EventRepository repository) {
        this.validator = validator;
        this.repository = repository;
    }

    public ReceiveOutcome receive(String rawJson) {
        EventEnvelope env = validator.parseAndValidate(rawJson);

        EventRecord existing = repository.findByEventId(env.eventId);
        if (existing != null) {
            log.info("接收判定 eventId={} partition={} attempt=N/A result=DUPLICATE_EVENT 判定依据=event_id 唯一约束, 既有 seq={}, status={}",
                    existing.eventId, existing.partition, existing.seqInPartition, existing.status);
            ReceiveOutcome o = new ReceiveOutcome();
            o.duplicate = true;
            o.eventId = existing.eventId;
            o.partition = existing.partition;
            o.seqInPartition = existing.seqInPartition;
            o.reason = com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason.DUPLICATE_EVENT;
            o.detail = "事件已存在，重复投递被幂等吸收";
            return o;
        }

        EventRecord saved = repository.insertIfAbsent(new EventRecord(
                env.eventId, env.partition, 0L, env.version, env.type, env.payload, env.priority));

        ReceiveOutcome o = new ReceiveOutcome();
        o.eventId = saved.eventId;
        o.partition = saved.partition;
        o.seqInPartition = saved.seqInPartition;
        o.duplicate = false;
        log.info("接收判定 eventId={} partition={} seq={} result=ACCEPTED 判定依据=校验通过+首次落库",
                saved.eventId, saved.partition, saved.seqInPartition);
        return o;
    }
}
