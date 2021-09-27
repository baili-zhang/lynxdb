package zbl.moonlight.cache.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Scanner;

public class MoonlightCacheClient {
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
                outputStream.writeUTF(command);
                outputStream.flush();

                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                byte[] bytes = byteBuffer.array();
                int n = inputStream.read(bytes);
                String response = new String(bytes, 0, n);
                System.out.println(response);

                if(response.trim().equals("[Close Connection]")) {
                    isConnectionHold = false;
                }
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
