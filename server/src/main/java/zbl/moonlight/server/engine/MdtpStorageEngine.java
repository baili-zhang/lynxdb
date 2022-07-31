package zbl.moonlight.server.engine;

import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.storage.core.KvAdapter;
import zbl.moonlight.storage.core.Pair;

import java.nio.charset.StandardCharsets;

import static zbl.moonlight.server.annotations.MdtpMethod.KV_SINGLE_GET;

public class MdtpStorageEngine extends BaseStorageEngine {
    public static final String C = "c";

    private static final byte KV_ADAPTER = (byte) 0x01;
    private static final byte TABLE_ADAPTER = (byte) 0x02;
    private static final byte META = (byte) 0x03;

    public MdtpStorageEngine() {
        super(MdtpStorageEngine.class);
    }

    public byte[] metaGet(String key) {
        return metaDb.get(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 为什么不用注解的方式？
     *  因为不是 Socket 通信的命令
     *  需要生成 byte[] 的 command
     *  再解析 command
     *  处理流程绕了一大圈
     *
     * @param key key
     * @param value value
     */
    public void metaSet(String key, byte[] value) {
        metaDb.set(new Pair<>(key.getBytes(StandardCharsets.UTF_8), value));
    }

    @MdtpMethod(KV_SINGLE_GET)
    private byte[] doSingleGetQuery(byte[] command) {
         String dbname = findKvDbName(command);
         KvAdapter kvDb = kvDbMap.get(dbname);

         if(kvDb == null) {
             throw new RuntimeException(dbname + " is not exist");
         }

         return kvDb.get(findKey(command));
    }

    protected byte findMethod(final byte[] command) {
        return (byte) 0x01;
    }

    private String findTableName(final byte[] command) {
        return null;
    }

    private String findKvDbName(final byte[] command) {
        return null;
    }

    private byte[] findKey(final byte[] command) {
        return null;
    }
}
