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

public enum RecordUnit {
    MILLIS("ms", (byte)0x01),
    NANOS("ns", (byte)0x02),
    TIMES("times", (byte) 0x03);

    final byte value;
    final String name;

    public static RecordUnit find(byte val) {
        RecordUnit[] units = values();

        for (RecordUnit unit : units) {
            if(unit.value == val) {
                return unit;
            }
        }

        throw new RuntimeException();
    }

    RecordUnit(String unitName, byte unitValue) {
        name = unitName;
        value = unitValue;
    }

    public byte value() {
        return value;
    }

    public String unitName() {
        return name;
    }
}
