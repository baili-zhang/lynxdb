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
