package zbl.moonlight.server.protocol;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Mdtp {
    public static final int HEADER_LENGTH = 6;

    private DynamicByteBuffer data;

    public Mdtp () {
        data = new DynamicByteBuffer();
    }

    public static ByteBuffer encode(byte code, ByteBuffer key, ByteBuffer value) throws EncodeException {
        if(key.limit() > 0xff) {
            throw new EncodeException("key length cannot exceed 0xff.");
        }
        byte keyLength = (byte) (key.limit() & 0xff);
        int valueLength = value.limit();
        ByteBuffer byteBuffer = ByteBuffer.allocate(HEADER_LENGTH + keyLength + valueLength);
        byteBuffer.put(code).put(keyLength).putInt(valueLength).put(key).put(value);
        return byteBuffer;
    }

    public Command decode () throws DecodeException {
        ByteBuffer first = data.getFirst();
        byte code = first.get(0);
        int keyLength = first.get(1) & 0xff;
        int valueLength = ((first.get(2) & 0xff) << 24) |
                ((first.get(3) & 0xff) << 16) |
                ((first.get(4) & 0xff) << 8) |
                (first.get(5) & 0xff);

        if(data.size() < 6 + keyLength + valueLength) {
            return null;
        } else if (data.size() > 6 + keyLength + valueLength) {
            throw new DecodeException("Invalid Command.");
        }

        Command command = new Command();
        ByteBuffer key = ByteBuffer.allocate(keyLength);
        data.copyTo(key, Mdtp.HEADER_LENGTH, keyLength);

        DynamicByteBuffer value = new DynamicByteBuffer();
        value.copyFrom(data, Mdtp.HEADER_LENGTH + keyLength, valueLength);

        command.setCode(code);
        command.setKey(key);
        command.setValue(value);

        return command;
    }

    public int read(SocketChannel socketChannel) throws IOException {
        ByteBuffer byteBuffer = data.last();
        while(true) {
            int readLength = socketChannel.read(byteBuffer);

            if (readLength == -1) return -1;
            if (readLength > 0) continue;

            if(data.isFull()) {
                byteBuffer = data.last();
                continue;
            }

            return data.size();
        }
    }
}
