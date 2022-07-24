package zbl.moonlight.server.mdtp;


import zbl.moonlight.storage.query.DeleteQuery;
import zbl.moonlight.storage.query.GetQuery;
import zbl.moonlight.storage.query.SetQuery;

public enum Method {
    KV_GET((byte) 0x01, GetQuery.class),
    KV_SET((byte) 0x02, SetQuery.class),
    KV_DELETE((byte) 0x03, DeleteQuery.class);

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
