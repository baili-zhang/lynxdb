package zbl.moonlight.server.eventbus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.server.cluster.RaftRole;
import zbl.moonlight.server.cluster.RaftState;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class EventBus implements Executable {
    private static final Logger logger = LogManager.getLogger("EventBus");
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 3000;

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
        long lastTimeMillis = System.currentTimeMillis();

        while (true) {
            int emptyQueueCount = 0, raftEventCount = 0;
            for (ConcurrentLinkedQueue<Event> queue : eventQueues.values()) {
                if(queue.isEmpty()) {
                    emptyQueueCount ++;
                    continue;
                }
                Event event = queue.poll();
                if(event.type() == EventType.RAFT_REQUEST) {
                    raftEventCount ++;
                }
                ConcurrentLinkedQueue<Executable> executors = registries.get(queue);
                /* 将事件分发给注册的执行器和对应线程 */
                for(Executable executor : executors) {
                    executor.offer(event);
                }
            }

            /* 如果在超过选取领导人时间之前没有收到来自当前领导人的AppendEntries RPC */
            /* 或者没有收到候选人的投票请求，则自己转换状态为候选人                  */
            long currentTimeMillis = System.currentTimeMillis();
            if(raftEventCount == 0 && currentTimeMillis > lastTimeMillis + DEFAULT_HEARTBEAT_INTERVAL) {
                RaftState raftState = MdtpServerContext.getInstance().getRaftState();
                raftState.setRaftRole(RaftRole.Candidate);
                logger.info("Set raft role to [RaftRole.Candidate], ready for leader election.");
                lastTimeMillis = currentTimeMillis;
            }

            /* 是否需要睡眠当前线程 */
            if(emptyQueueCount == eventQueues.size()) {
                synchronized (eventQueues) {
                    try {
                        eventQueues.wait(DEFAULT_HEARTBEAT_INTERVAL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void offer(Event event) {
        EventType type = event.type();
        ConcurrentLinkedQueue<Event> queue = eventQueues.get(type);
        queue.offer(event);
        synchronized (eventQueues) {
            eventQueues.notify();
        }
    }
}
