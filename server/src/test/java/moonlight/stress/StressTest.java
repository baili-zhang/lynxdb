package moonlight.stress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

class Task implements Runnable {

    private static final int PORT = 7820;
    private static final String HOST = "127.0.0.1";
    private static volatile AtomicInteger count = new AtomicInteger(0);
    private static volatile AtomicInteger fail = new AtomicInteger(0);

    @Override
    public void run() {
        try {
            Socket socket = new Socket(HOST, PORT);
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            for (int i = 0; i < 1; i++) {
                String command = "set " + Thread.currentThread().getName() + "-" + i + " " + Thread.currentThread().getName() + "-" + i;
                outputStream.writeUTF(command);
                outputStream.flush();

                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                byte[] bytes = byteBuffer.array();
                int n = inputStream.read(bytes);
                String response = new String(bytes, 0, n);
                if(!response.trim().split("\\s+")[0].equals("[OK]")) {
                    System.out.println(Thread.currentThread().getName() + "-" + i + " " + response);
                    fail.getAndIncrement();
                    if(fail.get() == 1) {
                        // System.out.println(count);
                    }
                } else {
                    count.getAndIncrement();
                }
            }
            inputStream.close();
            outputStream.close();
            socket.close();

            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

public class StressTest {
    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        for (int i = 0; i < 3600; i++) {
            new Thread(new Task()).start();
        }

        while (Thread.activeCount() > 2) {
            Thread.yield();
        }

        Date end = new Date();

        System.out.println("[TIME] " + (end.getTime() - begin.getTime()));
    }
}
