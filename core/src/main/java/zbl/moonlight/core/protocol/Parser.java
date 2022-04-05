package zbl.moonlight.core.protocol;

import lombok.Setter;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.schema.SchemaUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

/** 解析的内容：不包括最前面的4个byte位的长度信息 */
public class Parser {
    /** 用来存储各个属性的map */
    protected HashMap<String, byte[]> map;
    /** 传输的数据，不包括数据长度 */
    @Setter
    protected ByteBuffer byteBuffer;
    /** 继承Parsable的接口 */
    private final Class<? extends Parsable> schemaClass;
    private boolean parsed = false;

    public Parser(Class<? extends Parsable> schemaClass) {
        this.schemaClass = schemaClass;
    }

    public byte[] mapGet(String name) {
        if(!parsed) {
            throw new RuntimeException("Can NOT get before parsing.");
        }
        return map.get(name);
    }

    public void parse() {
        Parsable schema = (Parsable) Proxy.newProxyInstance(NioReader.class.getClassLoader(),
                new Class[]{schemaClass}, new ParseHandler());
        /* 把ByteBuffer类型的数据解析成map */
        map = schema.parse(byteBuffer);
        parsed = true;
    }

    /* 解析操作的JDK动态代理InvocationHandler */
    private class ParseHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if(!(args[0] instanceof ByteBuffer data)) {
                throw new IllegalStateException("args[0] is not a instance of ByteBuffer.");
            }
            List<SchemaEntry> schemaEntries = SchemaUtils.listAll(schemaClass);
            SchemaUtils.sort(schemaEntries);

            data.rewind();
            HashMap<String, byte[]> map = new HashMap<>();

            for (SchemaEntry entry : schemaEntries) {
                byte[] bytes;
                switch (entry.type()) {
                    case BYTE -> {
                        bytes = new byte[1];
                    }
                    case INT -> {
                        bytes = new byte[4];
                    }
                    case STRING -> {
                        int length = data.getInt();
                        bytes = new byte[length];
                    }
                    default -> throw new RuntimeException("Unsupported SchemaEntry type.");
                }

                data.get(bytes);
                map.put(entry.name(), bytes);
            }

            assert data.position() == data.limit();
            assert data.position() == data.capacity();

            return map;
        }
    }
}
