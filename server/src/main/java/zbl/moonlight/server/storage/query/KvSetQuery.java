package zbl.moonlight.server.storage.query;

import zbl.moonlight.server.storage.core.KeyValueStorable;

public record KvSetQuery(
        KeyValueStorable keyValueStorage,
        byte[] key,
        byte[] value
) implements Queryable {
    @Override
    public ResultSet query() {
        return keyValueStorage.kvSet(this);
    }
}
