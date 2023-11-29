package com.bailizhang.lynxdb.server.measure;

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
    RecordOption ENGINE_DO_QUERY_TIME = new RecordOption(
            "Engine Do Query Time",
            RecordUnit.NANOS
    );
    RecordOption ENGINE_QUERY_COUNT = new RecordOption(
            "Engine Query Count",
            RecordUnit.TIMES
    );
}
