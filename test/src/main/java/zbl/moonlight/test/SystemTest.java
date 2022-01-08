package zbl.moonlight.test;

import zbl.moonlight.client.common.Command;
import zbl.moonlight.client.exception.InvalidCommandException;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class SystemTest {
    private static DataInputStream inputStream;
    private static DataOutputStream outputStream;
    private static int identifier;

    public static void main(String[] args) {
        final int clientCount = 1;
        final String host = "127.0.0.1";
        final int port = 7820;
        final int commandCount = 1000000;

        for (int i = 0; i < clientCount; i++) {
            new Thread(() -> {
                try {
                    Socket socket = new Socket(host, port);
                    int count = 0;
                    inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                    outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

                    String content = "java.util.concurrent.ConcurrentSkipListSet";
                    for (int j = 0; j < 8; j++) {
                        content += content;
                    }

                    Date begin = new Date();
                    for (int j = 0; j < commandCount; j++) {
                        String setCommand = "set " + j + " " + content + j;
                        Command command = new Command(setCommand);
                        send(command);
                        if(get() == (byte)0x03) {
                            count ++;
                        }
                    }
                    Date end = new Date();
                    System.out.println(end.getTime() - begin.getTime());
                    System.out.println(count);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private static void send(Command command) throws IOException, InvalidCommandException {
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
        outputStream.writeInt(++ identifier);
        outputStream.write(key);
        outputStream.write(value);
        outputStream.flush();
    }

    private static byte get() throws IOException {
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
