package com.bailizhang.lynxdb.core.health;

import com.bailizhang.lynxdb.core.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FlightDataRecorder {
    public static final String READ_DATA_FROM_SOCKET = "Read Data from Socket";
    public static final String WRITE_DATA_TO_SOCKET = "Write Data to Socket";
    public static final String CLIENT_READ_DATA_FROM_SOCKET = "Client Read Data from Socket";
    public static final String CLIENT_WRITE_DATA_TO_SOCKET = "Client Write Data to Socket";
    public static final String ENGINE_DO_QUERY_TIME = "Engine Do Query Time";
    public static final String ENGINE_QUERY_COUNT = "Engine Query Count";

    private static final FlightDataRecorder RECORDER = new FlightDataRecorder();

    private final ConcurrentHashMap<String, AtomicLong> map = new ConcurrentHashMap<>();

    private Boolean enable;

    private FlightDataRecorder() {

    }

    public static FlightDataRecorder recorder() {
        return RECORDER;
    }

    public static void enable(boolean val) {
        if(RECORDER.enable == null) {
            RECORDER.enable = val;
        }
    }

    public boolean isEnable() {
        return enable != null && enable;
    }

    public void recordE(IRunnable task, String name) throws Exception {
        long beginNano = System.nanoTime();
        task.run();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(name, n -> new AtomicLong(0L));
        data.addAndGet(costNano);
    }

    public <T> T record(ICallable<T> task, String name) {
        long beginNano = System.nanoTime();
        T outcome = task.call();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(name, n -> new AtomicLong(0L));
        data.addAndGet(costNano);

        return outcome;
    }

    public <T> T record(Callable<T> task, String name) throws Exception {
        long beginNano = System.nanoTime();
        T outcome = task.call();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(name, n -> new AtomicLong(0L));
        data.addAndGet(costNano);

        return outcome;
    }

    public void count(String name) {
        AtomicLong data = map.computeIfAbsent(name, n -> new AtomicLong(0L));
        data.incrementAndGet();
    }

    public List<Pair<String, Long>> data() {
        List<Pair<String, Long>> list = new ArrayList<>();
        map.forEach((name, value) -> list.add(new Pair<>(name, value.get())));
        return list;
    }
}
