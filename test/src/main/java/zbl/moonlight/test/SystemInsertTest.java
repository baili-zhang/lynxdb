package zbl.moonlight.test;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class SystemInsertTest {
    private static final AtomicLong successCount = new AtomicLong(0);
    private static final int clientCount = 20;
    private static final String host = "127.0.0.1";
    private static final int port = 7820;
    private static final int commandCount = 10000;
    private static final CountDownLatch latch = new CountDownLatch(clientCount);

    public static void main(String[] args) {
        Date begin = new Date();
        for (int i = 0; i < commandCount; i = i + (commandCount/clientCount)) {
            final int beginIndex = i;
            new Thread(new ClientThread(host, port, latch, beginIndex,
                    commandCount/clientCount, successCount)).start();
        }
        try {
            latch.await();
            Date end = new Date();
            System.out.println(commandCount / (end.getTime() - begin.getTime()) * 1000);
            System.out.println(successCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
