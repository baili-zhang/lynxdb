package zbl.moonlight.core.protocol.nio;

import lombok.Getter;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.Readable;
import zbl.moonlight.core.protocol.annotations.Schema;
import zbl.moonlight.core.protocol.annotations.SchemaEntry;
import zbl.moonlight.core.utils.ByteBufferUtils;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

/** 协议的读策略，需要提供继承Parsable的接口 */
public class NioReader implements Readable {
    /** 保持socket连接 */
    public static final byte STAY_CONNECTED = (byte) 0x00;
    /** 断开socket连接 */
    public static final byte DISCONNECT = (byte) 0x01;

    private static final int SOCKET_HEADER_LENGTH = 5;

    /** 用来存储各个属性的map */
    private HashMap<String, byte[]> map;
    /** 传输的数据，不包括数据长度 */
    private ByteBuffer data;
    private final ByteBuffer socketHeader = ByteBuffer.allocate(SOCKET_HEADER_LENGTH);
    /** 是否保持连接的标志 */
    private boolean keepConnection = true;
    /** 继承Protocol的接口 */
    private final Class<? extends Parsable> schemaClass;
    @Getter
    private final SelectionKey selectionKey;
    /** 是否已完成parse */
    private boolean parseFlag = false;

    /** 给读数据用的构造函数 */
    public NioReader(Class<? extends Parsable> schemaClass, SelectionKey selectionKey) {
        this.schemaClass = schemaClass;
        this.selectionKey = selectionKey;
    }

    public byte[] mapGet(String name) {
        if(!parseFlag) {
            throw new RuntimeException("Can NOT get before parsing.");
        }
        return map.get(name);
    }

    @Override
    public void read() throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        if (!ByteBufferUtils.isOver(socketHeader)) {
            socketChannel.read(socketHeader);
            if(!ByteBufferUtils.isOver(socketHeader)) {
                return;
            }

            socketHeader.rewind();
            data = ByteBuffer.allocate(socketHeader.getInt());
            keepConnection = socketHeader.get() == STAY_CONNECTED;
        }

        if(!ByteBufferUtils.isOver(data)) {
            socketChannel.read(data);
            /* 如果读取完成，则解析读取的数据 */
            if(isReadCompleted()) {
                parse();
            }
        }
    }

    @Override
    public boolean isReadCompleted() {
        return ByteBufferUtils.isOver(data);
    }

    private void parse() {
        Parsable schema = (Parsable) Proxy.newProxyInstance(NioReader.class.getClassLoader(),
                new Class[]{schemaClass}, new ParseHandler());
        /* 把ByteBuffer类型的数据解析成map */
        map = schema.parse(data);
        parseFlag = true;
    }

    public boolean isKeepConnection() {
        return keepConnection;
    }

    /* 解析操作的JDK动态代理InvocationHandler */
    private class ParseHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if(!(args[0] instanceof ByteBuffer data)) {
                throw new IllegalStateException("args[0] is not a instance of ByteBuffer.");
            }
            Schema schema = schemaClass.getAnnotation(Schema.class);
            SchemaEntry[] schemaEntries = schema.value();

            data.rewind();
            HashMap<String, byte[]> map = new HashMap<>();

            for (SchemaEntry entry : schemaEntries) {
                if (entry.hasLengthSize()) {
                    /* TODO:禁止魔数（"4", "1"） */
                    int length;
                    if (entry.lengthSize() == 4) {
                        length = data.getInt();
                    } else if (entry.lengthSize() == 1) {
                        /* byte转int，防止最高位为1时，变成负数 */
                        length = data.get() & 0xff;
                    } else {
                        throw new IllegalStateException("lengthSize can only be 4 (for int) or 1 (for byte), as type \"int\"");
                    }
                    byte[] value = new byte[length];
                    data.get(value);
                    map.put(entry.name(), value);
                } else {
                    if (entry.length() < 0) {
                        throw new IllegalStateException("length can not be less than 0");
                    }
                    byte[] value = new byte[entry.length()];
                    data.get(value);
                    map.put(entry.name(), value);
                }
            }
            return map;
        }
    }
}
