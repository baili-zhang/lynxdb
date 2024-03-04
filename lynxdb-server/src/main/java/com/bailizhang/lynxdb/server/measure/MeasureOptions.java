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

package com.bailizhang.lynxdb.server.measure;

import com.bailizhang.lynxdb.core.recorder.RecordOption;
import com.bailizhang.lynxdb.core.recorder.RecordUnit;

public interface MeasureOptions {
    RecordOption ENGINE_DO_QUERY_TIME = new RecordOption(
            "Engine Do Query Time",
            RecordUnit.NANOS
    );
    RecordOption ENGINE_QUERY_COUNT = new RecordOption(
            "Engine Query Count",
            RecordUnit.TIMES
    );
}
