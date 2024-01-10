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

package com.bailizhang.lynxdb.socket.measure;

import com.bailizhang.lynxdb.core.recorder.RecordOption;
import com.bailizhang.lynxdb.core.recorder.RecordUnit;

public interface MeasureOptions {
    RecordOption READ_DATA_FROM_SOCKET = new RecordOption(
            "Read Data from Socket",
            RecordUnit.NANOS
    );
    RecordOption WRITE_DATA_TO_SOCKET = new RecordOption(
            "Write Data to Socket",
            RecordUnit.NANOS
    );
    RecordOption CLIENT_READ_DATA_FROM_SOCKET = new RecordOption(
            "Client Read Data from Socket",
            RecordUnit.NANOS
    );
    RecordOption CLIENT_WRITE_DATA_TO_SOCKET = new RecordOption(
            "Client Write Data to Socket",
            RecordUnit.NANOS
    );
}
