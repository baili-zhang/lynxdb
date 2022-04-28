package zbl.moonlight.core.raft.log;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class EntryIndexTest {

    @Test
    void fromBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(EntryIndex.ENTRY_INDEX_LENGTH);
        buffer.putInt(1).putInt(2).putInt(3).putInt(4);
        EntryIndex index = EntryIndex.fromBytes(buffer.array());
        assert index.term() == 1;
        assert index.commitIndex() == 2;
        assert index.offset() == 3;
        assert index.length() == 4;
    }

    @Test
    void toBytes() {
    }
}