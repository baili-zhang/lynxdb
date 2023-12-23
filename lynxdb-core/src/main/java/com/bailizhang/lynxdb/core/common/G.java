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

public class G {
    public static final G I = new G();

    private Converter converter;

    private G() {

    }

    public void converter(Converter cvt) {
        if(converter == null) {
            converter = cvt;
        }
    }

    public byte[] toBytes(String src) {
        return converter.toBytes(src);
    }

    public String toString(byte[] src) {
        return src == null ? null : converter.toString(src);
    }
}
