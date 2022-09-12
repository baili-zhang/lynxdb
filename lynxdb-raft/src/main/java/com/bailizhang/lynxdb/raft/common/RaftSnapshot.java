package com.bailizhang.lynxdb.raft.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.util.HashMap;

public class RaftSnapshot implements BytesListConvertible {
    private static final byte KV_STORE = (byte) 0x01;
    private static final byte TABLE = (byte) 0x02;

    private final HashMap<String, byte[]> kvMap = new HashMap<>();
    private final HashMap<String, byte[]> tableMap = new HashMap<>();

    public RaftSnapshot() {
    }

    public void kvstore(String name, byte[] data) {
        kvMap.put(name, data);
    }

    public void table(String name, byte[] data) {
        tableMap.put(name, data);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        for(String kv : kvMap.keySet()) {
            bytesList.appendRawByte(KV_STORE);
            bytesList.appendVarStr(kv);

            byte[] data = kvMap.get(kv);
            if(data == null) {
                data = BufferUtils.EMPTY_BYTES;
            }

            bytesList.appendVarBytes(data);
        }

        for(String table : tableMap.keySet()) {
            bytesList.appendRawByte(TABLE);
            bytesList.appendVarStr(table);

            byte[] data = tableMap.get(table);
            if(data == null) {
                data = BufferUtils.EMPTY_BYTES;
            }

            bytesList.appendRawBytes(data);
        }

        return bytesList;
    }
}
