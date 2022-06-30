package zbl.moonlight.server.storage.query.cf;

import zbl.moonlight.server.storage.core.AbstractNioQuery;

import java.nio.channels.SelectionKey;

public abstract class CfQuery extends AbstractNioQuery implements CfQueryable {
    protected final byte[] columnFamily;
    protected final byte[] key;

    protected CfQuery(SelectionKey selectionKey, byte[] columnFamily, byte[] key) {
        super(selectionKey);
        this.columnFamily = columnFamily;
        this.key = key;
    }

    public byte[] columnFamily() {
        return columnFamily;
    }

    public byte[] key() {
        return key;
    }

}
