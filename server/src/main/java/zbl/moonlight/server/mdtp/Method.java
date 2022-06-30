package zbl.moonlight.server.mdtp;

import zbl.moonlight.server.storage.query.cf.CfDeleteQuery;
import zbl.moonlight.server.storage.query.cf.CfGetQuery;
import zbl.moonlight.server.storage.query.cf.CfSetQuery;
import zbl.moonlight.server.storage.query.kv.KvDeleteQuery;
import zbl.moonlight.server.storage.query.kv.KvGetQuery;
import zbl.moonlight.server.storage.query.kv.KvSetQuery;

public enum Method {
    KV_GET((byte) 0x01, KvGetQuery.class),
    KV_SET((byte) 0x02, KvSetQuery.class),
    KV_DELETE((byte) 0x03, KvDeleteQuery.class),
    CF_GET((byte) 0x04, CfGetQuery.class),
    CF_SET((byte) 0x05, CfSetQuery.class),
    CF_DELETE((byte) 0x06, CfDeleteQuery.class);

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
