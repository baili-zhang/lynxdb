package rcache.worker;

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
                String command = inputStream.readUTF();
                System.out.println("RCache server receive data: " + command);

                if(command.equals("exit")) {
                    System.out.println("Receive commend exit");
                    isConnectionHold = false;

                    outputStream.writeUTF("close");
                    outputStream.flush();
                } else {
                    outputStream.writeUTF(command + " has received !" );
                    outputStream.flush();
                }
            }

            System.out.println("Socket close");
            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
