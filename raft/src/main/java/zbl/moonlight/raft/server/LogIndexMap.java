package zbl.moonlight.raft.server;

import java.nio.channels.SelectionKey;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LogIndexMap {
    private final ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<Integer>> map;

    LogIndexMap() {
        map = new ConcurrentHashMap<>();
    }

    public void offer(SelectionKey key, Integer logIndex) {
        ConcurrentLinkedQueue<Integer> queue = createQueueIfNotExist(key);
        queue.offer(logIndex);
    }

    public Integer peek(SelectionKey key) {
        ConcurrentLinkedQueue<Integer> queue = createQueueIfNotExist(key);
        return queue.peek();
    }

    public Integer peekAfterPoll(SelectionKey key) {
        ConcurrentLinkedQueue<Integer> queue = createQueueIfNotExist(key);
        queue.poll();
        return queue.peek();
    }

    public Set<SelectionKey> keySet() {
        return map.keySet();
    }

    private ConcurrentLinkedQueue<Integer> createQueueIfNotExist(SelectionKey key) {
        ConcurrentLinkedQueue<Integer> queue = map.get(key);
        if(queue == null) {
            queue = new ConcurrentLinkedQueue<>();
            map.put(key, queue);
        }

        return queue;
    }
}
