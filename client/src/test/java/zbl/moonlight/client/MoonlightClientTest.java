package zbl.moonlight.client;

import org.junit.jupiter.api.Test;
import zbl.moonlight.client.common.Command;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class MoonlightClientTest {

    @Test
    void run() throws Exception {
        ConcurrentLinkedQueue<Command> queue = new ConcurrentLinkedQueue<>();
        MoonlightClient client = new MoonlightClient("127.0.0.1", 7820, queue);
        client.run();
        for (int i = 0; i < 200; i++) {
            client.send("set key value is " + i);
        }
    }

    @Test
    void send() {
    }
}