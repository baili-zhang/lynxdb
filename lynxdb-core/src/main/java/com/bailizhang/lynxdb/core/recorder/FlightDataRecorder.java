/*
 * Copyright 2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.core.recorder;

import com.bailizhang.lynxdb.core.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// TODO 设计的有问题，需要重写设计
public class FlightDataRecorder {
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
        if(!isEnable()) {
            task.run();
            return;
        }

        long beginNano = System.nanoTime();
        task.run();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(option, n -> new AtomicLong(0L));
        data.addAndGet(costNano);
    }

    public <T> T record(ICallable<T> task, RecordOption option) {
        if(!isEnable()) {
             return task.call();
        }

        long beginNano = System.nanoTime();
        T outcome = task.call();
        long endNano = System.nanoTime();

        long costNano = endNano - beginNano;
        AtomicLong data = map.computeIfAbsent(option, n -> new AtomicLong(0L));
        data.addAndGet(costNano);

        return outcome;
    }

    public <T> T record(Callable<T> task, RecordOption option) throws Exception {
        if(!isEnable()) {
            return task.call();
        }

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
