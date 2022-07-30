package zbl.moonlight.server.engine;

import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.storage.core.KvAdapter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static zbl.moonlight.server.annotations.MdtpMethod.KV_SINGLE_GET;

public class MdtpStorageEngine extends BaseStorageEngine {
    public static final String C = "c";
    public static final String C_OLD_NEW = "c_old_new";

    private static final byte KV_ADAPTER = (byte) 0x01;
    private static final byte TABLE_ADAPTER = (byte) 0x02;
    private static final byte META = (byte) 0x03;

    public MdtpStorageEngine() {
        super(MdtpStorageEngine.class);
    }

    public byte[] doQuery(byte[] command) {
        if(command == null) {
            throw new RuntimeException("Command is null.");
        }

        byte method = findMethod(command);

        Method doQueryMethod = methodMap.get(method);
        if(doQueryMethod == null) {
            throw new RuntimeException("Not Supported mdtp method.");
        }

        try {
            return (byte[]) doQueryMethod.invoke(this, command);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] metaGet(String key) {
        return metaDb.get(key.getBytes(StandardCharsets.UTF_8));
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

    private byte findMethod(final byte[] command) {
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
