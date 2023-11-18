package com.bailizhang.lynxdb.core.log;

public record LogEntry(
        LogIndex index,
        byte[] data
) {

}
