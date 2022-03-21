package zbl.moonlight.server.protocol;

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
/* TODO: 根据单一职责原则，读和写应该在两个类中实现       */
public abstract class AbstractProtocol implements Transportable {
    private static final int NO_LENGTH = -1;

    /* 传输的数据，不包括数据长度 */
    private ByteBuffer data;
    /* 数据总长度 */
    private int length = NO_LENGTH;
    /* 数据总长度ByteBuffer */
    private ByteBuffer lengthByteBuffer = ByteBuffer.allocate(4);
    /* 用来存储各个属性的map */
    private HashMap<String, byte[]> map;
    /* 继承Protocol的接口 */
    private final Class<? extends ProtocolSchema> schemaClass;

    /* 给读数据用的构造函数 */
    AbstractProtocol(Class<? extends ProtocolSchema> schemaClass) {
        this.schemaClass = schemaClass;
    }

    /* 给写数据用的构造函数 */
    AbstractProtocol(Class<? extends ProtocolSchema> schemaClass, byte[] data) {
        this.schemaClass = schemaClass;
        this.data = ByteBuffer.wrap(data);
        this.length = data.length;
        lengthByteBuffer.putInt(this.length);
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
    public void write(SocketChannel socketChannel) throws IOException {
        if(!ByteBufferUtils.isOver(lengthByteBuffer)) {
            socketChannel.write(lengthByteBuffer);
            if(!ByteBufferUtils.isOver(lengthByteBuffer)) {
                return;
            }
        }
        if(!ByteBufferUtils.isOver(data)) {
            socketChannel.read(data);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return ByteBufferUtils.isOver(data);
    }

    @Override
    public boolean isWriteCompleted() {
        return ByteBufferUtils.isOver(data);
    }

    /* JDK动态代理的InvocationHandler */
    private class InvH implements InvocationHandler {
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

    public void parse() {
        ProtocolSchema schema = (ProtocolSchema) Proxy.newProxyInstance(MdtpRequest.class.getClassLoader(),
                new Class[]{MdtpRequestSchema.class}, new InvH());
        /* 把ByteBuffer类型的数据解析成map */
        map = schema.parse(data);
    }
}
