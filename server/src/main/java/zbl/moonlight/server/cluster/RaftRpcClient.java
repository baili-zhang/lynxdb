package zbl.moonlight.server.cluster;

import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.executor.Executable;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RaftRpcClient implements Executable {
    @Override
    public void offer(Event event) {

    }

    @Override
    public void run() {
        try {
            Socket socket = new Socket("127.0.0.1", 7850);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(new RequestVoteRpc.Arguments(1,
                    1,1,2,
                    "hhhh".getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
