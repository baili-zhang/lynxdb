package zbl.moonlight.server.storage.query;

import zbl.moonlight.server.storage.core.ColumnFamilyStorable;

public record CfSetQuery(
        ColumnFamilyStorable columnFamilyStorage,
        byte[] columnFamily,
        byte[] key,
        byte[] value
) implements CfQueryable {
    @Override
    public ResultSet query() {
        return columnFamilyStorage.cfSet(this);
    }
}
