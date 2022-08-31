package zbl.moonlight.starter;

import com.bailizhang.lynxdb.client.AsyncMoonlightClient;
import com.bailizhang.lynxdb.client.MoonlightFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;

public class MoonlightTemplate {
    private final AsyncMoonlightClient client;
    private final SelectionKey current;

    public MoonlightTemplate(MoonlightProperties properties) {
        client = new AsyncMoonlightClient();
        Executor.start(client);
        ServerNode server = new ServerNode(properties.getHost(), properties.getPort());

        try {
            current = client.connect(server);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public MoonlightFuture asyncKvGet(String kvstore, List<byte[]> keys) {
        return client.asyncKvGet(current, kvstore, keys);
    }
}
