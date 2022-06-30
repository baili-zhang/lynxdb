package zbl.moonlight.server.storage.query.cf;

import zbl.moonlight.server.storage.query.cf.CfQuery;

import java.nio.channels.SelectionKey;

public abstract class CfQueryWithValue extends CfQuery {
    protected final byte[] value;

    protected CfQueryWithValue(SelectionKey selectionKey, byte[] columnFamily, byte[] key, byte[] value) {
        super(selectionKey, columnFamily, key);
        this.value = value;
    }

    public byte[] value() {
        return value;
    }
}
