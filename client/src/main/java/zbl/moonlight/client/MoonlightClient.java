package zbl.moonlight.client;

import zbl.moonlight.server.command.Method;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MoonlightClient {
    private static final int PORT = 7820;
    private static final String HOST = "127.0.0.1";

    private static boolean isConnectionHold = true;

    public static void main(String[] args) throws IOException {
        Socket socket = new Socket(HOST, PORT);
        DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

        Scanner scanner = new Scanner(System.in);

        try {
            while (isConnectionHold) {
                System.out.print("Moonlight > ");

                String command = scanner.nextLine();
                String[] commandArray = command.trim().split("\\s+");
                byte code = (byte) 0xff;
                ByteBuffer key = null, value = null;

                switch (commandArray[0]) {
                    case "get":
                        code = Method.GET;
                        break;
                    case "set":
                        code = Method.SET;
                        break;
                    case "update":
                        code = Method.UPDATE;
                        break;
                    case "delete":
                        code = Method.DELETE;
                        break;
                    case "exit":
                        code = Method.EXIT;
                        break;
                }

                if (commandArray.length == 1) {
                    key = ByteBuffer.wrap(commandArray[1].getBytes(StandardCharsets.UTF_8));
                    byte keyLength = (byte) key.capacity();
                    int valueLength = 0;

                    ByteBuffer request = ByteBuffer.allocate(1 + 1 + keyLength + 4 + valueLength);
                    request.put(code);
                    request.put(keyLength);
                    request.put(key);
                    request.putInt(valueLength);
                    outputStream.write(request.array());
                }

                if (commandArray.length == 2) {
                    key = ByteBuffer.wrap(commandArray[1].getBytes(StandardCharsets.UTF_8));
                    value = ByteBuffer.wrap(commandArray[1].getBytes(StandardCharsets.UTF_8));
                    byte keyLength = (byte) key.capacity();
                    int valueLength = value.capacity();

                    ByteBuffer request = ByteBuffer.allocate(1 + 1 + keyLength + 4 + valueLength);
                    request.put(code);
                    request.put(keyLength);
                    request.put(key);
                    request.putInt(valueLength);
                    request.put(value);
                    outputStream.write(request.array());
                }

                outputStream.flush();

                byte[] status = new byte[1];
                inputStream.read(status);
                int valueLength = inputStream.readInt();
                byte[] responseValue = new byte[valueLength];
                inputStream.read(responseValue);

                System.out.println(new String(responseValue));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Connection closing...");
            socket.close();
            scanner.close();
        }
    }
}
