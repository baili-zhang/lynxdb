package com.bailizhang.lynxdb.client;

import java.nio.channels.SelectionKey;

public class LynxDbConnection {
    private SelectionKey current;

    public LynxDbConnection() {

    }

    public void current(SelectionKey key) {
        current = key;
    }

    public SelectionKey current() {
        return current;
    }
}
