package zbl.moonlight.server.mdtp;


import zbl.moonlight.storage.rocks.query.kv.KvBatchDeleteQuery;
import zbl.moonlight.storage.rocks.query.kv.KvBatchGetQuery;
import zbl.moonlight.storage.rocks.query.kv.KvBatchSetQuery;

public enum Method {
    KV_GET((byte) 0x01, KvBatchGetQuery.class),
    KV_SET((byte) 0x02, KvBatchSetQuery.class),
    KV_DELETE((byte) 0x03, KvBatchDeleteQuery.class);

    private byte value;
    private Class<?> type;

    Method(byte value, Class<?> type) {
        this.value = value;
        this.type = type;
    }

    public byte value() {
        return value;
    }

    public Class<?> type() {
        return type;
    }
}
