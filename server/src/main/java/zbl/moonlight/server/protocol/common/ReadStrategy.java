package zbl.moonlight.server.protocol.common;

import zbl.moonlight.server.protocol.mdtp.MdtpRequestSchema;
import zbl.moonlight.server.protocol.annotations.Schema;
import zbl.moonlight.server.protocol.annotations.SchemaEntry;
import zbl.moonlight.server.utils.ByteBufferUtils;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

/* 抽象协议，为MDTP请求和响应协议提供支持，不能同时用来读写 */
public class ReadStrategy implements Readable {
    private static final int NO_LENGTH = -1;

    /* 用来存储各个属性的map */
    private HashMap<String, byte[]> map;
    /* 传输的数据，不包括数据长度 */
    private ByteBuffer data;
    /* 数据总长度 */
    private int length = NO_LENGTH;
    /* 数据总长度ByteBuffer */
    private ByteBuffer lengthByteBuffer = ByteBuffer.allocate(4);
    /* 继承Protocol的接口 */
    private final Class<? extends Parsable> schemaClass;
    /* 是否已完成parse */
    private boolean parseFlag = false;

    /* 给读数据用的构造函数 */
    public ReadStrategy(Class<? extends Parsable> schemaClass) {
        this.schemaClass = schemaClass;
    }

    public byte[] mapGet(String name) {
        if(!parseFlag) {
            throw new RuntimeException("Can NOT get before parsing.");
        }
        return map.get(name);
    }

    @Override
    public void read(SocketChannel socketChannel) throws IOException {
        if (length == NO_LENGTH) {
            socketChannel.read(lengthByteBuffer);
            if(!ByteBufferUtils.isOver(lengthByteBuffer)) {
                return;
            }
        }

        length = lengthByteBuffer.getInt(0);
        data = ByteBuffer.allocate(length);

        if(!ByteBufferUtils.isOver(data)) {
            socketChannel.read(data);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return ByteBufferUtils.isOver(data);
    }

    public void parse() {
        if(!isReadCompleted()) {
            throw new RuntimeException("Can NOT parse before reading completed.");
        }
        Parsable schema = (Parsable) Proxy.newProxyInstance(ReadStrategy.class.getClassLoader(),
                new Class[]{MdtpRequestSchema.class}, new ParseHandler());
        /* 把ByteBuffer类型的数据解析成map */
        map = schema.parse(data);
    }

    /* 解析操作的JDK动态代理InvocationHandler */
    private class ParseHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if(!(args[0] instanceof ByteBuffer)) {
                throw new IllegalStateException("args[0] is not a instance of ByteBuffer.");
            }
            Schema schema = schemaClass.getAnnotation(Schema.class);
            SchemaEntry[] schemaEntries = schema.value();

            ByteBuffer data = (ByteBuffer) args[0];
            data.rewind();
            HashMap<String, byte[]> map = new HashMap<>();

            for (int i = 0; i < schemaEntries.length; i++) {
                SchemaEntry entry = schemaEntries[i];
                if(entry.hasLengthSize()) {
                    /* TODO:禁止魔数（"4"） */
                    if(entry.lengthSize() != 4) {
                        throw new IllegalStateException("lengthSize can only be 4, as type \"int\"");
                    }
                    int length = data.getInt();
                    byte[] value = new byte[length];
                    data.get(value);
                    map.put(entry.name(), value);
                } else {
                    if(entry.length() < 0) {
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
