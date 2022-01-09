package zbl.moonlight.test;

import zbl.moonlight.client.common.Command;
import zbl.moonlight.client.exception.InvalidCommandException;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class SystemTest {
    public static void main(String[] args) {
        final int clientCount = 10;
        final String host = "127.0.0.1";
        final int port = 7820;
        final int commandCount = 1000000;

        final CountDownLatch latch = new CountDownLatch(clientCount);
        Date begin = new Date();
        for (int i = 0; i < commandCount; i = i + (commandCount/clientCount)) {
            final int beginIndex = i;
            new Thread(() -> {
                try {
                    int identifier = 0;
                    Socket socket = new Socket(host, port);
                    System.out.println("connect to server: " + beginIndex);
                    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                    DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                    String content = "java.util.concurrent.ConcurrentSkipListSet";
                    for (int j = 0; j < 0; j++) {
                        content += content;
                    }

                    for (int j = beginIndex; j < beginIndex + (commandCount/clientCount); j++) {
                        String setCommand = "set " + j + " " + content + j;
                        Command command = new Command(setCommand);
                        send(command, outputStream, ++ identifier);
                    }

                    for (int j = 0; j < commandCount/clientCount; j++) {
                        try {
                            get(inputStream);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        try {
            latch.await();
            Date end = new Date();
            System.out.println(commandCount / (end.getTime() - begin.getTime()) * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void send(Command command, DataOutputStream outputStream, int identifier) throws IOException, InvalidCommandException {
        byte method = command.getCode();
        byte[] key = command.getKey().toString().getBytes(StandardCharsets.UTF_8);
        byte[] value = command.getValue().toString().getBytes(StandardCharsets.UTF_8);
        if(key.length > 255) {
            throw new InvalidCommandException("key is too long.");
        }
        byte keyLength = (byte) key.length;
        int valueLength = value.length;

        /* 写方法和键的长度 */
        outputStream.write(new byte[]{method, keyLength});
        /* 写值的长度 */
        outputStream.writeInt(valueLength);
        outputStream.writeInt(identifier);
        outputStream.write(key);
        outputStream.write(value);
        outputStream.flush();
    }

    private static byte get(DataInputStream inputStream) throws IOException {
        byte responseCode = inputStream.readByte();
        int valueLength = inputStream.readInt();
        int identifier = inputStream.readInt();
        String responseValue = "";
        if(valueLength != 0) {
            byte[] responseValueBytes = new byte[valueLength];
            inputStream.read(responseValueBytes);
            responseValue += new String(responseValueBytes);
        }
        return responseCode;
    }
}
