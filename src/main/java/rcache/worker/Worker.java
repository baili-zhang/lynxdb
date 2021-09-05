package rcache.worker;

import rcache.engine.StringHashTable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class Worker implements Runnable {
    private Socket socket;
    private boolean isConnectionHold;

    private StringHashTable stringHashTable = new StringHashTable();

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
                String[] commandArray = commandLine.split(" ");

                String command = null, key = null, value = null;

                if(commandArray.length > 0) {
                    command = commandArray[0];
                }

                if(commandArray.length > 1) {
                    key = commandArray[1];
                }

                if(commandArray.length > 2) {
                    value = commandArray[2];
                }


                if(command != null && command.equals("exit")) {
                    isConnectionHold = false;

                    outputStream.writeUTF("close");
                    outputStream.flush();

                    break;
                }

                if(command != null && command.equals("set") && key != null && value != null) {
                    stringHashTable.set(key, value);
                    outputStream.writeUTF("[set] " + key + ": " + value);
                }

                if(command != null && command.equals("get") && key != null) {
                    String result = stringHashTable.get(key);
                    outputStream.writeUTF(result);
                }

                if(command != null && command.equals("delete") && key != null) {
                    stringHashTable.delete(key);
                    outputStream.writeUTF("[delete] " + key);
                }

                outputStream.flush();
            }

            System.out.println("Socket close");
            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
