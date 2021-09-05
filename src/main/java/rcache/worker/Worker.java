package rcache.worker;

import rcache.engine.CommandResolver;
import rcache.engine.Executor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class Worker implements Runnable {
    private Socket socket;
    private boolean isConnectionHold;

    public Worker(Socket socket) {
        this.socket = socket;
        this.isConnectionHold = true;
    }

    @Override
    public void run() {
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            while (isConnectionHold) {
                String commandLine = inputStream.readUTF();
                String[] commandArray = CommandResolver.line(commandLine);
                isConnectionHold = Executor.execute(commandArray, outputStream);
                outputStream.flush();
            }

            System.out.println("Socket close");
            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
