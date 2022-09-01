package com.bailizhang.lynxdb.storage.core;

import com.bailizhang.lynxdb.storage.rocks.query.Query;

public interface Database extends AutoCloseable {
    ResultSet<?> doQuery(Query<?, ?> query) throws Exception;
}
