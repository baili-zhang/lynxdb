package com.bailizhang.lynxdb.storage.rocks.query;

import com.bailizhang.lynxdb.storage.core.ResultSet;
import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

public abstract class Query<QD, R> implements Queryable {
    protected final QD queryData;
    protected final ResultSet<R> resultSet;

    protected List<ColumnFamilyHandle> columnFamilyHandles;

    protected ColumnFamilyHandle defaultHandle;

    protected Query(QD queryData, ResultSet<R> resultSet) {
        this.queryData = queryData;
        this.resultSet = resultSet;
    }

    public void setColumnFamilyHandles(List<ColumnFamilyHandle> handles) {
        columnFamilyHandles = handles;
    }

    public void setDefaultHandle(ColumnFamilyHandle defaultHandle) {
        this.defaultHandle = defaultHandle;
    }

    public ResultSet<R> resultSet() {
        return resultSet;
    }
}
