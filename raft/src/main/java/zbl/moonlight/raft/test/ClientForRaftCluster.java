package zbl.moonlight.raft.test;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.request.RaftRequest;
import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.client.SocketClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ClientForRaftCluster {
    public static void main(String[] args) throws IOException, InterruptedException {
        SocketClient client = new SocketClient();
        ServerNode node = new ServerNode("127.0.0.1", 7822);
        client.connect(node);
        byte[] command = "set bb hallo world!".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(command.length + 2);
        buffer.put(RaftRequest.CLIENT_REQUEST).put(ClientRequest.RAFT_CLIENT_REQUEST_SET).put(command);

        client.offer(SocketRequest.newUnicastRequest(buffer.array(), node));
        client.setHandler(new SocketClientHandler() {
            @Override
            public void handleConnected(ServerNode node) {
            }

            @Override
            public void handleResponse(AbstractSocketResponse response) throws Exception {
                System.out.println(response.data().length);
                System.out.println(response.data()[0]);
            }
        });
        Executor.start(client);

        TimeUnit.SECONDS.sleep(30);
    }
}
