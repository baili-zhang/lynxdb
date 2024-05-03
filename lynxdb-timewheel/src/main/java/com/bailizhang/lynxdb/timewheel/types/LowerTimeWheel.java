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

package com.bailizhang.lynxdb.timewheel.types;

import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.util.List;

public class LowerTimeWheel extends TimeWheel {
    private final TimeWheel nextTimeWheel;

    public LowerTimeWheel(int scale, int totalTime, TimeWheel next) {
        super(scale, totalTime);
        nextTimeWheel = next;
    }

    @Override
    public int init(long time) {
        int remain = nextTimeWheel.init(time);
        slot = remain / millisPerSlot;
        baseTime = time - remain;

        return remain % millisPerSlot;
    }

    @Override
    public int register(TimeoutTask task) {
        int remain = nextTimeWheel.register(task);

        // 已经注册成功了
        if(remain < 0) {
            return SUCCESS;
        }

        int newSlot = remain / millisPerSlot;
        if(newSlot > slot) {
            circle[newSlot].add(task);
            return SUCCESS;
        } else if(newSlot < slot) {
            return 0;
        }

        return remain % millisPerSlot;
    }

    @Override
    public int unregister(TimeoutTask task) {
        int remain = nextTimeWheel.unregister(task);

        // 已经移除成功了
        if(remain < 0) {
            return SUCCESS;
        }

        int newSlot = remain / millisPerSlot;
        if(newSlot > slot) {
            circle[newSlot].remove(task);
            return SUCCESS;
        } else if(newSlot < slot) {
            return 0;
        }

        return remain % millisPerSlot;
    }

    @Override
    protected List<TimeoutTask> nextRound() {
        return nextTimeWheel.tick();
    }
}
