package zbl.moonlight.starter;

import com.bailizhang.lynxdb.client.AsyncLynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;

public class LynxDbTemplate {
    private final AsyncLynxDbClient client;
    private final SelectionKey current;

    public LynxDbTemplate(LynxDbProperties properties) {
        client = new AsyncLynxDbClient();
        Executor.start(client);
        ServerNode server = new ServerNode(properties.getHost(), properties.getPort());

        try {
            current = client.connect(server);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public LynxDbFuture asyncKvGet(String kvstore, List<byte[]> keys) {
        return client.asyncKvGet(current, kvstore, keys);
    }
}
