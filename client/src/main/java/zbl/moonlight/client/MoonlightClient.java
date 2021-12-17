package zbl.moonlight.client;

import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.Mdtp;

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
                System.out.print("Moonlight > ");

                String command = scanner.nextLine();
                String[] commandArray = command.trim().split("\\s+");
                byte code = (byte) 0xff;
                ByteBuffer key = ByteBuffer.wrap(commandArray[1].getBytes(StandardCharsets.UTF_8));
                ByteBuffer value = ByteBuffer.wrap(commandArray[2].getBytes(StandardCharsets.UTF_8));

                switch (commandArray[0]) {
                    case "get":
                        code = MdtpMethod.GET;
                        break;
                    case "set":
                        code = MdtpMethod.SET;
                        break;
                    case "update":
                        code = MdtpMethod.UPDATE;
                        break;
                    case "delete":
                        code = MdtpMethod.DELETE;
                        break;
                    case "exit":
                        code = MdtpMethod.EXIT;
                        break;
                }

                outputStream.write(Mdtp.encode(code, key, value).array());
                outputStream.flush();

                byte status = inputStream.readByte();
                int valueLength = inputStream.readInt();
                byte[] responseValue = new byte[valueLength];
                inputStream.read(responseValue);

                System.out.println(new String(responseValue));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Connection closing...");
            socket.close();
            scanner.close();
        }
    }
}
