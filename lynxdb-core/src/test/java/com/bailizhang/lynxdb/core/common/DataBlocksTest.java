package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

class DataBlocksTest {

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    @Test
    void appendRawByte() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte((byte)0x01);
        dataBlocks.appendRawByte((byte)0x02);
        ByteBuffer[] buffers = dataBlocks.toBuffers();

        assert buffers.length == 2;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x01});
        assert Arrays.equals(buffers[1].array(), new byte[]{0x02});
        assert BufferUtils.length(buffers) == 2;
    }

    @Test
    void appendRawBytes() {
        DataBlocks dataBlocks = new DataBlocks(true);
        dataBlocks.appendRawBytes(new byte[]{0x01, 0x02});
        dataBlocks.appendRawBytes(new byte[]{0x03, 0x04});
        ByteBuffer[] buffers = dataBlocks.toBuffers();

        assert buffers.length == 3;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, 0x04});
        assert Arrays.equals(buffers[1].array(), new byte[]{0x01, 0x02});
        assert Arrays.equals(buffers[2].array(), new byte[]{0x03, 0x04});
        assert BufferUtils.length(buffers) == 8;
    }

    @Test
    void appendRawBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);
        ByteBuffer[] origin = BufferUtils.toBuffers(
                new byte[]{0x01, 0x02},
                new byte[]{0x03, 0x04}
        );
        dataBlocks.appendRawBuffers(origin);
        ByteBuffer[] buffers = dataBlocks.toBuffers();

        assert buffers.length == 3;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, 0x04});
        assert Arrays.equals(buffers[1].array(), new byte[]{0x01, 0x02});
        assert Arrays.equals(buffers[2].array(), new byte[]{0x03, 0x04});
        assert BufferUtils.length(buffers) == 8;
    }

    @Test
    void appendRawStr() {
        DataBlocks dataBlocks = new DataBlocks(true);

        String exam = "example";
        byte[] examBytes = G.I.toBytes(exam);

        dataBlocks.appendRawStr(exam);

        ByteBuffer[] buffers = dataBlocks.toBuffers();

        assert buffers.length == 2;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, (byte)examBytes.length});
        assert exam.equals(G.I.toString(buffers[1].array()));
        assert BufferUtils.length(buffers) == INT_LENGTH + examBytes.length;
    }

    @Test
    void appendRawInt() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawInt(5);
        ByteBuffer[] buffers = dataBlocks.toBuffers();
        assert buffers.length == 1;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, 0x05});
        assert BufferUtils.length(buffers) == 4;
    }

    @Test
    void appendRawLong() {
        DataBlocks dataBlocks = new DataBlocks(true);
        dataBlocks.appendRawLong(5);
        ByteBuffer[] buffers = dataBlocks.toBuffers();
        assert buffers.length == 2;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, 0x08});
        assert Arrays.equals(buffers[1].array(), new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05});
        assert BufferUtils.length(buffers) == 12;
    }

    @Test
    void appendVarBytes() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendVarBytes(new byte[]{0x00, 0x01, 0x02});
        ByteBuffer[] buffers = dataBlocks.toBuffers();
        assert buffers.length == 2;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, 0x03});
        assert Arrays.equals(buffers[1].array(), new byte[]{0x00, 0x01, 0x02});
        assert BufferUtils.length(buffers) == 7;
    }

    @Test
    void appendVarStr() {
        DataBlocks dataBlocks = new DataBlocks(false);

        String exam = "example";
        byte[] examBytes = G.I.toBytes(exam);

        dataBlocks.appendVarStr(exam);

        ByteBuffer[] buffers = dataBlocks.toBuffers();

        assert buffers.length == 2;
        assert Arrays.equals(buffers[0].array(), new byte[]{0x00, 0x00, 0x00, (byte)examBytes.length});
        assert exam.equals(G.I.toString(buffers[1].array()));
        assert BufferUtils.length(buffers) == INT_LENGTH + examBytes.length;
    }
}