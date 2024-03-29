/*
 * Copyright 2022 Baili Zhang.
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

package com.bailizhang.lynxdb.core.common;

import java.util.Arrays;

public class WrappedBytes implements Comparable<WrappedBytes>{
    private final byte[] bytes;

    public WrappedBytes(byte[] val) {
        bytes = val;
    }

    public byte[] bytes() {
        return bytes;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrappedBytes that = (WrappedBytes) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(WrappedBytes o) {
        return Arrays.compare(bytes, o.bytes);
    }
}
