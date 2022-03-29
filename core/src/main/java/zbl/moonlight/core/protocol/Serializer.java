package zbl.moonlight.core.protocol;

import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.protocol.schema.SchemaUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * 序列化器
 */
public class Serializer {
    protected HashMap<String, byte[]> map = new HashMap<>();
    protected ByteBuffer byteBuffer;
    /** 继承MSerializable的接口 */
    private final Class<? extends MSerializable> schemaClass;

    public Serializer(Class<? extends MSerializable> schemaClass) {
        this.schemaClass = schemaClass;
    }

    /* 往map中添加数据 */
    public void mapPut(String name, byte[] data) {
        map.put(name, data);
    }

    /* 序列化操作的JDK动态代理InvocationHandler */
    private class SerializeHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            List<SchemaEntry> schemaEntries = SchemaUtils.listAll(schemaClass);
            SchemaUtils.sort(schemaEntries);
            int length = 4;

            /* 先遍历一遍求序列化后的总长度 */
            for(SchemaEntry entry : schemaEntries) {
                if(!map.containsKey(entry.name())) {
                    throw new IllegalCallerException("map not contains key \"" + entry.name() + "\"");
                }

                switch (entry.type()) {
                    case BYTE -> length += 1;
                    case INT -> length += 4;
                    case STRING -> length += (4 + map.get(entry.name()).length);
                }
            }

            ByteBuffer data = ByteBuffer.allocate(length);
            data.putInt(length - 4);
            /* 序列化数据 */
            for(SchemaEntry entry : schemaEntries) {
                byte[] bytes = map.get(entry.name());

                switch (entry.type()) {
                    case BYTE -> data.put(bytes[0]);
                    case INT -> data.put(Arrays.copyOfRange(bytes, 0, 4));
                    case STRING -> data.putInt(bytes.length).put(bytes);
                }
            }
            return data;
        }
    }

    protected void serialize() {
        MSerializable schema = (MSerializable) Proxy.newProxyInstance(NioWriter.class.getClassLoader(),
                new Class[]{schemaClass}, new SerializeHandler());
        /* 将map序列化成ByteBuffer */
        byteBuffer = schema.serialize(map);
        byteBuffer.rewind();
    }

    public ByteBuffer getByteBuffer() {
        serialize();
        return byteBuffer;
    }
}
