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

public interface NameUtils {
    int DEFAULT_NAME_LENGTH = 8;
    String ZERO = "0";

    static String name(int id) {
        String idStr = String.valueOf(id);
        int zeroCount = DEFAULT_NAME_LENGTH - idStr.length();
        return ZERO.repeat(Math.max(0, zeroCount)) + idStr;
    }

    static int id(String name) {
        try {
            if(name.charAt(DEFAULT_NAME_LENGTH) != '.') {
                throw new RuntimeException();
            }

            return Integer.parseInt(name.substring(0, DEFAULT_NAME_LENGTH));
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }
}
