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
