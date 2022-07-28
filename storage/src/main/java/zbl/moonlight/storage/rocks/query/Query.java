package zbl.moonlight.storage.rocks.query;

import org.rocksdb.ColumnFamilyHandle;
import zbl.moonlight.storage.core.ResultSet;

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
