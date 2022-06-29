package zbl.moonlight.server.storage.query;

import zbl.moonlight.server.storage.core.ColumnFamilyStorable;

public record CfDeleteQuery(
        ColumnFamilyStorable columnFamilyStorage,
        byte[] columnFamily,
        byte[] key
) implements CfQueryable {
    @Override
    public ResultSet query() {
        return columnFamilyStorage.cfDelete(this);
    }
}
