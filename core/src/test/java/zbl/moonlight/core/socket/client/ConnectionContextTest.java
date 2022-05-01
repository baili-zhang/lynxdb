package zbl.moonlight.core.socket.client;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionContextTest {
    @Test
    void testLinkedQueue() {
        ConcurrentLinkedQueue<Object> objects = new ConcurrentLinkedQueue<>();
        Object nullObject = new Object();
        objects.offer(1);
        objects.offer(nullObject);
        objects.offer(nullObject);
        objects.offer(3);

        assert objects.poll().equals(1);
        assert objects.poll().equals(nullObject);
        assert objects.poll().equals(nullObject);
        assert objects.poll().equals(3);
    }
}