package zbl.moonlight.core.raft.log;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class EntryIndexTest {

    @Test
    void fromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(EntryIndex.ENTRY_INDEX_LENGTH);
        buffer.putInt(1).putInt(2).putInt(3);
        EntryIndex index = EntryIndex.fromBytes(buffer.array());
        assert index.term() == 1;
        assert index.offset() == 2;
        assert index.length() == 3;
    }

    @Test
    void toBytes() {
    }
}