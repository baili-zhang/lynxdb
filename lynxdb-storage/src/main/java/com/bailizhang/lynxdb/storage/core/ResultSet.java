package com.bailizhang.lynxdb.storage.core;

public class ResultSet<T> {
    private long lastSequenceNumber;

    private T result;

    public void setResult(T result) {
        this.result = result;
    }

    public T result() {
        return result;
    }
}
