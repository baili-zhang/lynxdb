package zbl.moonlight.cache.server;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapTest {
    private static final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    public static void main(String[] args) {

        final Date begin = new Date();

        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000000; j++) {
                    map.put(Thread.currentThread().getName() + j, Thread.currentThread().getName() + j);
                    Date end = new Date();
                    System.out.println(map.size() + " " + (end.getTime() - begin.getTime()));
                }
            }).start();
        }

        if (Thread.activeCount() > 2) {
            Thread.yield();
        }
    }
}
