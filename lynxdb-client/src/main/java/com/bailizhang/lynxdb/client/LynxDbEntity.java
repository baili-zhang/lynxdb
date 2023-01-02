package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.lsmtree.common.DbValue;

import java.util.List;

public interface LynxDbEntity {
    byte[] key();
    byte[] columnFamily();
    byte[] column();
    byte[] value();

    LynxDbClient client();

    default byte[] find() {
        return client().find(key(), columnFamily(), column());
    }

    default List<DbValue> findByKey() {
        return client().find(key(), columnFamily());
    }

    default void delete() {
        client().delete(key(), columnFamily(), column());
    }

    default void insert() {
        client().insert(key(), columnFamily(), column(), value());
    }

    default void register() {
        client().register(key(), columnFamily());
    }

    default void deregister() {
        client().deregister(key(), columnFamily());
    }
}
