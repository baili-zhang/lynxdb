package zbl.moonlight.server.cluster;

import lombok.Getter;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

/* 集群响应的组织器 */
public class ResponseOrganizer extends Executor<Event<?>> {
    @Getter
    private final String NAME = "ResponseOrganizer";
    private final ConcurrentHashMap<SelectionKey, ConcurrentHashMap<Integer, Event<?>>>
            countMap = new ConcurrentHashMap<>();

    private final int count;

    public ResponseOrganizer(int count, EventBus eventBus, Thread eventBusThread) {
        super(eventBus, eventBusThread);
        this.count = count;
    }

    @Override
    public void run() {
        while (true) {
            Event<?> event = pollSleep();
            if(event == null) {
                continue;
            }
            MdtpResponse response = (MdtpResponse) event.getValue();
            check(event.getSelectionKey(), event, response.getIdentifier());
        }
    }

    /* 检查并更新已经发回响应的数量 */
    private void check (SelectionKey selectionKey, Event<?> responseEvent, int identifier) {
        ConcurrentHashMap<Integer, Event<?>> responses = countMap.get(selectionKey);
        if(responses == null) {
            responses = new ConcurrentHashMap<>();
            countMap.put(selectionKey, responses);
        }
        Event<?> event = responses.get(identifier);
        if(event == null) {
            event = responseEvent;
            event.setType(EventType.CLUSTER_RESPONSE);
            responses.put(identifier, event);
        }
        /* TODO:需要判断请求是否成功，如果时超时则不自增计数器 */
        event.responseCountIncrement();
        if(event.getClusterResponseCount() >= count) {
            responses.remove(identifier);
            send(event);
        }
    }
}
