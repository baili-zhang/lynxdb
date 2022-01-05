package zbl.moonlight.server.eventbus;

import lombok.Getter;
import lombok.Setter;

import java.nio.channels.SelectionKey;

@Getter
public class Event<T> {
    @Setter
    private EventType type;

    private final SelectionKey selectionKey;
    private final T value;

    /* 集群候选人节点发回响应的数量 */
    @Getter
    private int clusterResponseCount;

    public Event(EventType type, SelectionKey selectionKey, T value) {
        this.type = type;
        this.selectionKey = selectionKey;
        this.value = value;
        this.clusterResponseCount = 0;
    }

    public void responseCountIncrement() {
        clusterResponseCount ++;
    }
}
