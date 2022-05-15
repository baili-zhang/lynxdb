package zbl.moonlight.core.raft.test;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.request.RaftRequest;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.request.SocketRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static zbl.moonlight.core.raft.server.RaftServerHandler.RAFT_CLIENT_REQUEST_SET;

public class ClientForRaftCluster {
    public static void main(String[] args) throws IOException, InterruptedException {
        SocketClient client = new SocketClient();
        ServerNode node = new ServerNode("127.0.0.1", 7820);
        client.connect(node);
        byte[] command = "set a value".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(command.length + 2);
        buffer.put(RaftRequest.CLIENT_REQUEST).put(RAFT_CLIENT_REQUEST_SET).put(command);

        client.offer(SocketRequest.newUnicastRequest(buffer.array(), node));
        client.setHandler(new SocketClientHandler() {
            @Override
            public void handleConnected(ServerNode node) {
            }
        });
        Executor.start(client);

        TimeUnit.SECONDS.sleep(30);
    }
}
