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

package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;

public interface ArrayUtils {
    static int compare(byte[] origin, byte[] target) {
        int minLen = Math.min(origin.length, target.length);

        for(int i = 0; i < minLen; i ++) {
            if(origin[i] > target[i]) {
                return 1;
            } else if(origin[i] < target[i]) {
                return -1;
            }
        }

        return origin.length - target.length;
    }

    static boolean isEmpty(Object[] src) {
        return src == null || src.length == 0;
    }

    static int toInt(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getInt();
    }

    static long toLong(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getLong();
    }

    static <T> T first(T[] arr) {
        if(arr.length == 0) {
            throw new IndexOutOfBoundsException();
        }
        return arr[0];
    }

    static <T> T last(T[] arr) {
        if(arr.length == 0) {
            throw new IndexOutOfBoundsException();
        }
        return arr[arr.length - 1];
    }
}
