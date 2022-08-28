package zbl.moonlight.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.server.MoonlightServer;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.storage.core.Pair;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

class AsyncMoonlightClientTest {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 7820;
    public static final String KV_STORE = "kv_store";

    @BeforeEach
    public void setUp() throws IOException {
        MoonlightServer.main(new String[0]);
    }

    @Test
    void test_001_asyncKvGet() throws IOException {
        AsyncMoonlightClient client = new AsyncMoonlightClient();
        Executor.start(client);
        SelectionKey selectionKey = client.connect(new ServerNode(HOST, PORT));
        MoonlightFuture future = client.asyncKvGet(selectionKey, KV_STORE, new ArrayList<>());
        byte[] result = future.get();
    }
}