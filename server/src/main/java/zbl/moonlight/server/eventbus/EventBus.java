package zbl.moonlight.server.eventbus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.executor.Executable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class EventBus implements Executable {
    private static final Logger logger = LogManager.getLogger("EventBus");

    private final ConcurrentHashMap<EventType, ConcurrentLinkedQueue<Event>> eventQueues
            = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<ConcurrentLinkedQueue<Event>,
            ConcurrentLinkedQueue<Executable>> registries
            = new ConcurrentHashMap<>();

    public EventBus() {
        for(EventType eventType : EventType.values()) {
            ConcurrentLinkedQueue<Event> queue = new ConcurrentLinkedQueue<>();
            eventQueues.put(eventType, queue);
            registries.put(queue, new ConcurrentLinkedQueue<>());
        }
    }

    public void register(EventType type, Executable executor) {
        ConcurrentLinkedQueue<Event> queue = eventQueues.get(type);
        ConcurrentLinkedQueue<Executable> executors = registries.get(queue);
        executors.add(executor);
    }

    @Override
    public void run() {
        while (true) {
            int emptyQueueCount = 0;
            for (ConcurrentLinkedQueue<Event> queue : eventQueues.values()) {
                if(queue.isEmpty()) {
                    emptyQueueCount ++;
                    continue;
                }
                Event event = queue.poll();
                ConcurrentLinkedQueue<Executable> executors = registries.get(queue);
                /* 将事件分发给注册的执行器和对应线程 */
                for(Executable executor : executors) {
                    executor.offer(event);
                }
            }
            if(emptyQueueCount == eventQueues.size()) {
                synchronized (eventQueues) {
                    try {
                        eventQueues.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void offer(Event event) {
        ConcurrentLinkedQueue<Event> queue = eventQueues.get(event.type());
        queue.offer(event);
        synchronized (eventQueues) {
            eventQueues.notify();
        }
    }
}
