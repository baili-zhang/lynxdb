package zbl.moonlight.client;

import com.bailizhang.lynxdb.client.AsyncLynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbFuture;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.LynxDbMainServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.storage.core.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.bailizhang.lynxdb.server.engine.result.Result.SUCCESS;
import static com.bailizhang.lynxdb.server.engine.result.Result.SUCCESS_WITH_KV_PAIRS;

class AsyncLynxDbClientTest {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 7820;
    public static final String KV_STORE = "kv_store";
    public static final String KEY = "key";
    public static final String VALUE = "value";

    public static AsyncLynxDbClient client;
    public static SelectionKey current;

    @BeforeAll
    public static void setUp() throws IOException, InterruptedException {
        LynxDbMainServer.main(new String[0]);
        TimeUnit.SECONDS.sleep(3);

        client = new AsyncLynxDbClient();
        Executor.start(client);
        current = client.connect(new ServerNode(HOST, PORT));
    }

    @Test
    void test_001_asyncCreateKvstore() {
        LynxDbFuture future = client.asyncCreateKvstore(current, List.of(KV_STORE));
        byte[] result = future.get();
        assert result[0] == SUCCESS;
    }

    @Test
    void test_001_asyncKvGet() {
        List<LynxDbFuture> futures = new ArrayList<>();

        for(int i = 0; i < 100; i ++) {
            byte[] key = G.I.toBytes(KEY + i);
            byte[] value = G.I.toBytes(VALUE + i);

            Pair<byte[], byte[]> pair = new Pair<>(key, value);
            LynxDbFuture fut = client.asyncKvSet(current, KV_STORE, List.of(pair));
            futures.add(fut);
        }

        for(LynxDbFuture fut : futures) {
            byte[] r = fut.get();
            assert r[0] == SUCCESS;
        }

        for(int i = 0; i < 100; i ++) {
            byte[] key = G.I.toBytes(KEY + i);
            LynxDbFuture fut = client.asyncKvGet(current, KV_STORE, List.of(key));
            byte[] r = fut.get();
            assert r[0] == SUCCESS_WITH_KV_PAIRS;
        }
    }
}