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

package com.bailizhang.lynxdb.ldtp.annotations;

public @interface LdtpCode {
    byte VOID           = (byte) 0x01;
    byte TRUE           = (byte) 0x02;
    byte FALSE          = (byte) 0x03;
    byte NULL           = (byte) 0x04;
    byte BYTE_ARRAY     = (byte) 0x05;
    byte MULTI_COLUMNS  = (byte) 0x06;
    byte MULTI_KEYS     = (byte) 0x07;
}
