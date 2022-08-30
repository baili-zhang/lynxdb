package zbl.moonlight.starter;

import zbl.moonlight.client.AsyncMoonlightClient;
import zbl.moonlight.client.MoonlightFuture;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.socket.client.ServerNode;

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
