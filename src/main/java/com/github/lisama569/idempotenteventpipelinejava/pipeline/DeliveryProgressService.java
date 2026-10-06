package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * 投递进度查询（只读）：不修改任何状态、不触发业务副作用。
 */
@Service
public class DeliveryProgressService {

    private final EventStore store;

    public DeliveryProgressService(EventStore store) {
        this.store = store;
    }

    /** 按事件标识查询当前投递进度；未知事件返回空。 */
    public Optional<DeliveryProgress> progressOf(String eventId) {
        return store.find(eventId).map(DeliveryProgressService::toProgress);
    }

    private static DeliveryProgress toProgress(EventStore.StoredEvent se) {
        DeliveryProgress.Stage stage = switch (se.status()) {
            case PENDING -> se.attempts() > 0
                    ? DeliveryProgress.Stage.RETRY_WAIT
                    : DeliveryProgress.Stage.PENDING_DELIVERY;
            case DELIVERED -> DeliveryProgress.Stage.DELIVERED;
            case REJECTED -> DeliveryProgress.Stage.REJECTED;
            case DEAD -> DeliveryProgress.Stage.DEAD;
        };
        Long nextAt = se.status() == EventStore.EventStatus.PENDING
                && se.nextAttemptAtMillis() > 0
                ? se.nextAttemptAtMillis() : null;
        return new DeliveryProgress(se.envelope().eventId(),
                se.envelope().partitionKey(), se.envelope().sequence(),
                stage, se.attempts(), nextAt, se.rejectionReason());
    }
}
