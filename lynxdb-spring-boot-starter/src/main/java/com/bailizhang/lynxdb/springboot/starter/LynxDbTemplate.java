package com.bailizhang.lynxdb.springboot.starter;

public class LynxDbTemplate extends AsyncLynxDbTemplate {
    public LynxDbTemplate(LynxDbProperties properties) {
        super(properties);
    }

    public <T> T kvGet(Class<T> clazz) {
        return client.kvGet(current, clazz);
    }

    public void kvSet(Object o) {
        client.kvSet(current, o);
    }
}
