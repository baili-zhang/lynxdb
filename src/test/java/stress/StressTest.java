package stress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

class Task implements Runnable {

    private static final int PORT = 7820;
    private static final String HOST = "127.0.0.1";

    @Override
    public void run() {
        try {
            Socket socket = new Socket(HOST, PORT);
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            for (int i = 0; i < 10000; i++) {
                String command = "set " + Thread.currentThread().getName() + "-" + i + " " + Thread.currentThread().getName() + "-" + i;
                outputStream.writeUTF(command);
                outputStream.flush();

                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                byte[] bytes = byteBuffer.array();
                int n = inputStream.read(bytes);
                String response = new String(bytes, 0, n);
                System.out.println(Thread.currentThread().getName() + " " + i + " " + response);
            }
            inputStream.close();
            outputStream.close();
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

public class StressTest {
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(new Task()).start();
        }
    }
}
