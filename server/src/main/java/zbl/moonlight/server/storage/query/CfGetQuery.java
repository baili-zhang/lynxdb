package zbl.moonlight.server.storage.query;

import zbl.moonlight.server.storage.core.ColumnFamilyStorable;

public record CfGetQuery(
        ColumnFamilyStorable columnFamilyStorage,
        String database,
        byte[] columnFamily,
        byte[] key
) implements CfQueryable {
    @Override
    public ResultSet query() {
        return columnFamilyStorage.cfGet(this);
    }
}
