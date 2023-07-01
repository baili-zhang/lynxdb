package com.bailizhang.lynxdb.core.health;

import com.bailizhang.lynxdb.core.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FlightDataRecorder {
    public static final RecordOption READ_DATA_FROM_SOCKET = new RecordOption(
            "Read Data from Socket",
            RecordUnit.NANOS
    );
    public static final RecordOption WRITE_DATA_TO_SOCKET = new RecordOption(
            "Write Data to Socket",
            RecordUnit.NANOS
    );
    public static final RecordOption CLIENT_READ_DATA_FROM_SOCKET = new RecordOption(
            "Client Read Data from Socket",
            RecordUnit.NANOS
    );
    public static final RecordOption CLIENT_WRITE_DATA_TO_SOCKET = new RecordOption(
            "Client Write Data to Socket",
            RecordUnit.NANOS
    );
    public static final RecordOption ENGINE_DO_QUERY_TIME = new RecordOption(
            "Engine Do Query Time",
            RecordUnit.NANOS
    );
    public static final RecordOption ENGINE_QUERY_COUNT = new RecordOption(
            "Engine Query Count",
            RecordUnit.TIMES
    );

    private static final FlightDataRecorder RECORDER = new FlightDataRecorder();

    private final ConcurrentHashMap<RecordOption, AtomicLong> map = new ConcurrentHashMap<>();

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

    public void recordE(IRunnable task, RecordOption option) throws Exception {
        long beginNano = System.nanoTime();
        task.run();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(option, n -> new AtomicLong(0L));
        data.addAndGet(costNano);
    }

    public <T> T record(ICallable<T> task, RecordOption option) {
        long beginNano = System.nanoTime();
        T outcome = task.call();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(option, n -> new AtomicLong(0L));
        data.addAndGet(costNano);

        return outcome;
    }

    public <T> T record(Callable<T> task, RecordOption option) throws Exception {
        long beginNano = System.nanoTime();
        T outcome = task.call();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(option, n -> new AtomicLong(0L));
        data.addAndGet(costNano);

        return outcome;
    }

    public void count(RecordOption option) {
        AtomicLong data = map.computeIfAbsent(option, n -> new AtomicLong(0L));
        data.incrementAndGet();
    }

    public List<Pair<RecordOption, Long>> data() {
        List<Pair<RecordOption, Long>> list = new ArrayList<>();
        map.forEach((option, value) -> list.add(new Pair<>(option, value.get())));
        return list;
    }
}
