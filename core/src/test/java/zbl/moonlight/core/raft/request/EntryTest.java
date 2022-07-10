package zbl.moonlight.core.raft.request;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class EntryTest {

    @Test
    void fromBytes() {
        int term = 2;
        byte[] command = "command".getBytes(StandardCharsets.UTF_8);

        int len = NumberUtils.INT_LENGTH + command.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(term).put(command);
        byte[] bytes = buffer.array();

        Entry entry = Entry.fromBytes(bytes);

        assert entry.term() == term;
        assert new String(entry.command()).equals("command");
    }

    @Test
    void toBytes() {
    }

    @Test
    void getDataBytes() {
    }
}