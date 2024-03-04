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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LdtpMethod {
    byte FIND_BY_KEY_CF_COLUMN          = (byte) 0x01;
    byte FIND_MULTI_COLUMNS             = (byte) 0x02;
    byte INSERT                         = (byte) 0x03;
    byte INSERT_MULTI_COLUMNS           = (byte) 0x04;
    byte INSERT_IF_NOT_EXISTED          = (byte) 0x05;
    byte DELETE                         = (byte) 0x06;
    byte RANGE_NEXT                     = (byte) 0x07;
    byte RANGE_BEFORE                   = (byte) 0x08;
    byte EXIST_KEY                      = (byte) 0x09;

    byte value();
}
