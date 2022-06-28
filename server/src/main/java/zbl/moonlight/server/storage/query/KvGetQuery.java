package zbl.moonlight.server.storage.query;

import zbl.moonlight.server.storage.core.KeyValueStorable;

public record KvGetQuery(
        KeyValueStorable keyValueStorage,
        String database,
        byte[] key
) implements Queryable {
    @Override
    public ResultSet query() {
        return keyValueStorage.kvGet(this);
    }
}
