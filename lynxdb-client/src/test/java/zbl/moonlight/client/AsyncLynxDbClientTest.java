package zbl.moonlight.client;

import com.bailizhang.lynxdb.client.AsyncLynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.LynxDbMainServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.bailizhang.lynxdb.server.engine.result.Result.Error.INVALID_ARGUMENT;

class AsyncLynxDbClientTest {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 7820;
    public static final String KV_STORE = "kv_store";

    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        LynxDbMainServer.main(new String[0]);
        TimeUnit.SECONDS.sleep(3);
    }

    @Test
    void test_001_asyncKvGet() throws IOException {
        AsyncLynxDbClient client = new AsyncLynxDbClient();
        Executor.start(client);
        SelectionKey selectionKey = client.connect(new ServerNode(HOST, PORT));

        List<byte[]> keys = List.of(G.I.toBytes("key"));

        LynxDbFuture future = client.asyncKvGet(selectionKey, KV_STORE, keys);
        byte[] result = future.get();

        assert result[0] == INVALID_ARGUMENT;
    }
}