/*
 * Copyright 2022-2023 Baili Zhang.
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

package com.bailizhang.lynxdb.core.executor;

/* TODO: 把一些在类中独立实现的 shutdown 逻辑替换为继承 Shutdown 类 */
public abstract class Shutdown {
    private volatile boolean shutdown = false;

    public void shutdown() {
        handleShutdown();
        shutdown = true;
    }

    public boolean isNotShutdown() {
        return !shutdown;
    }

    protected void handleShutdown() {

    }
}
