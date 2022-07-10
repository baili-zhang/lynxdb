package zbl.moonlight.storage.query.kv;

import java.nio.channels.SelectionKey;

public abstract class KvQueryWithValue extends KvQuery {
    protected final byte[] value;

    protected KvQueryWithValue(SelectionKey selectionKey, byte[] key, byte[] value) {
        super(selectionKey, key);
        this.value = value;
    }
}
