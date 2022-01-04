package zbl.moonlight.server;

import java.util.concurrent.TimeUnit;

public class ThreadTest {
    public static void main(String[] args) {
        Thread t = new Thread(() -> {
            while (true) {
                System.out.println("sleep");
                try {
                    TimeUnit.SECONDS.sleep(200);
                } catch (InterruptedException e) {
                    System.out.println("InterruptedException");
                }
            }
        });

        t.start();
        System.out.println(t.isInterrupted());

        new Thread(() -> {
            t.interrupt();
            System.out.println(t.isInterrupted());
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(t.isInterrupted());
            t.interrupt();
        }).start();
    }
}
