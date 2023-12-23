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

package com.bailizhang.lynxdb.core.utils;

public interface PrimitiveTypeUtils {
    int BYTE_LENGTH = 1;
    int CHAR_LENGTH = 2;
    int SHORT_LENGTH = 2;
    int INT_LENGTH = 4;
    int FLOAT_LENGTH = 4;
    int LONG_LENGTH = 8;
    int DOUBLE_LENGTH = 8;

    int BYTE_BIT_COUNT = 8;
}
