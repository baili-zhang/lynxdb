package zbl.moonlight.server.cluster;

import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.executor.Executable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class RaftRpcServer implements Executable {
    /* Raft Rpc 的默认端口号 */
    private final int DEFAULT_PORT = 7850;
    /* Raft Rpc 的端口号 */
    private final int port = DEFAULT_PORT;

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);

            while (true) {
                Socket socket = serverSocket.accept();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

                Object object = ois.readObject();
                RequestVoteRpc.Arguments arguments = object instanceof RequestVoteRpc.Arguments
                        ? ((RequestVoteRpc.Arguments) object) : null;
                assert arguments != null;
                System.out.println(new String(arguments.bytes()));

                oos.writeObject(new RequestVoteRpc.Results());
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void offer(Event event) {

    }
}
