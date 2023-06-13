package com.bailizhang.lynxdb.core.health;

import com.bailizhang.lynxdb.core.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FlightDataRecorder {
    public static final String READ_DATA_FROM_SOCKET = "Read Data from Socket";
    public static final String WRITE_DATA_TO_SOCKET = "Write Data to Socket";
    public static final String CLIENT_READ_DATA_FROM_SOCKET = "Client Read Data from Socket";
    public static final String CLIENT_WRITE_DATA_TO_SOCKET = "Client Write Data to Socket";
    public static final String ENGINE_DO_QUERY = "Engine Do Query";

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

    public List<Pair<String, Long>> data() {
        List<Pair<String, Long>> list = new ArrayList<>();
        map.forEach((name, value) -> list.add(new Pair<>(name, value.get())));
        return list;
    }
}
