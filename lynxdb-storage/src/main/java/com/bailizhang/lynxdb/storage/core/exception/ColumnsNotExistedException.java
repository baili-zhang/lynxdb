package com.bailizhang.lynxdb.storage.core.exception;

import com.bailizhang.lynxdb.storage.core.Column;

import java.util.HashSet;

public class ColumnsNotExistedException extends RuntimeException {
    private final HashSet<String> columns = new HashSet<>();

    public ColumnsNotExistedException() {
        super();
    }

    public void addColumn(byte[] column) {
        columns.add(new String(column));
    }

    public void addColumn(Column column) {
        columns.add(new String(column.value()));
    }

    public HashSet<String> columns() {
        return columns;
    }

    public boolean isNotEmpty() {
        return !columns.isEmpty();
    }
}
