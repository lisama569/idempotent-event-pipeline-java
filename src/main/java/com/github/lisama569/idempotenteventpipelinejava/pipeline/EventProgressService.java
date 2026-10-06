package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * 事件投递进度查询（只读）。
 *
 * <p>只读取持久化状态推导进度视图，不修改任何状态、不触发业务副作用；
 * 在途（PENDING）与重试等待中（RETRY_WAIT）的事件同样可查。
 */
@Service
public class EventProgressService {

    private final EventStore store;

    public EventProgressService(EventStore store) {
        this.store = store;
    }

    /** 按事件标识查询当前进度；未知事件返回空。 */
    public Optional<EventProgress> progress(String eventId) {
        return store.find(eventId)
                .map(stored -> EventProgress.from(stored, System.currentTimeMillis()));
    }
}
