package zbl.moonlight.core.raft.request;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class EntryTest {

    @Test
    void fromBytes() {
        int term = 2;
        int commitIndex = 4;
        byte method = RaftRequest.REQUEST_VOTE;
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        int len = NumberUtils.INT_LENGTH * 4 + NumberUtils.BYTE_LENGTH + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(term).putInt(commitIndex).put(method).putInt(key.length)
                .put(key).putInt(value.length).put(value);
        byte[] bytes = buffer.array();

        Entry entry = Entry.fromBytes(bytes);

        assert entry.term() == term;
        assert entry.commitIndex() == commitIndex;
        assert entry.method() == method;
        assert new String(entry.key()).equals("key");
        assert new String(entry.value()).equals("value");
    }

    @Test
    void toBytes() {
    }

    @Test
    void getDataBytes() {
    }
}