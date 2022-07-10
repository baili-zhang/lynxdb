package zbl.moonlight.storage.query.kv;

import zbl.moonlight.storage.core.AbstractNioQuery;

import java.nio.channels.SelectionKey;

public abstract class KvQuery extends AbstractNioQuery {
    protected final byte[] key;

    protected KvQuery(SelectionKey selectionKey, byte[] key) {
        super(selectionKey);
        this.key = key;
    }

    public byte[] key() {
        return key;
    }
}
