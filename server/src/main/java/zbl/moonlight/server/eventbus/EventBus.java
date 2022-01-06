package zbl.moonlight.server.eventbus;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.executor.Executable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class EventBus implements Executable<Event<?>> {
    @Getter
    private final String NAME = "EventBus";
    private final Logger logger = LogManager.getLogger("EventBus");

    private final ConcurrentHashMap<EventType, ConcurrentLinkedQueue<Event<?>>> eventQueues
            = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<ConcurrentLinkedQueue<Event<?>>,
            ConcurrentHashMap<Executable<Event<?>>, Thread>> registries
            = new ConcurrentHashMap<>();

    public EventBus() {
        for(EventType eventType : EventType.values()) {
            ConcurrentLinkedQueue<Event<?>> queue = new ConcurrentLinkedQueue<>();
            eventQueues.put(eventType, queue);
            registries.put(queue, new ConcurrentHashMap<>());
        }
    }

    public void register(EventType type, Executable<Event<?>> executor, Thread notifiedThread) {
        ConcurrentLinkedQueue<Event<?>> queue = eventQueues.get(type);
        ConcurrentHashMap<Executable<Event<?>>, Thread> registry = registries.get(queue);
        registry.put(executor, notifiedThread);
    }

    @Override
    public void run() {
        while (true) {
            int emptyQueueCount = 0;
            for (ConcurrentLinkedQueue<Event<?>> queue : eventQueues.values()) {
                if(queue.isEmpty()) {
                    emptyQueueCount ++;
                    continue;
                }
                Event<?> event = queue.poll();
                ConcurrentHashMap<Executable<Event<?>>, Thread> registry = registries.get(queue);
                /* 将事件分发给注册的执行器和对应线程 */
                for(Executable<Event<?>> executor : registry.keySet()) {
                    executor.offer(event);
                    Thread notifiedThread = registry.get(executor);
                    if(Thread.State.TIMED_WAITING.equals(notifiedThread.getState())) {
                        notifiedThread.interrupt();
                    } else if(Thread.State.NEW.equals(notifiedThread.getState())) {
                        logger.info("Thread \"{}\" State is NEW.", notifiedThread.getName());
                    }
                }
            }
            if(emptyQueueCount == eventQueues.size()) {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void offer(Event<?> event) {
        ConcurrentLinkedQueue<Event<?>> queue = eventQueues.get(event.getType());
        queue.offer(event);
    }

    /* 启动所有注册的线程 */
    public void start() {
        for(ConcurrentHashMap<Executable<Event<?>>, Thread> registry : registries.values()) {
            for(Thread thread : registry.values()) {
                thread.start();
            }
        }
    }
}
