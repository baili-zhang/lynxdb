package rcache.worker;

import rcache.engine.Cacheable;
import rcache.executor.Command;
import rcache.executor.Executor;
import rcache.executor.ResultSet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class Worker implements Runnable {
    private Socket socket;
    private boolean isConnectionHold;
    private Cacheable cacheEngine;

    public Worker(Socket socket, Cacheable cacheEngine) {
        this.socket = socket;
        this.isConnectionHold = true;
        this.cacheEngine = cacheEngine;
    }

    @Override
    public void run() {
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            while (isConnectionHold) {
                String commandLine = inputStream.readUTF();

                Command command = new Command(commandLine);
                command.resolve();

                Executor executor = new Executor(cacheEngine);
                ResultSet resultSet = executor.execute(command);

                isConnectionHold = resultSet.isConnectionHold();

                outputStream.writeUTF(resultSet.getResponse().format());
                outputStream.writeUTF("OVER");

                outputStream.flush();
            }

            System.out.println("Socket close");
            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
