package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.annotations.LdtpCode;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.bailizhang.lynxdb.server.annotations.LdtpCode.DB_VALUE_LIST;
import static com.bailizhang.lynxdb.server.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.server.annotations.LdtpMethod.*;

public class LdtpStorageEngine extends BaseStorageEngine {
    public LdtpStorageEngine() {
        super(LdtpStorageEngine.class);
    }

    @LdtpMethod(FIND_BY_KEY_CF_COLUMN)
    public QueryResult doFindByKeyCfColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);

        byte[] value = lsmTree.find(key, columnFamily, column);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(LdtpCode.BYTE_ARRAY);
        bytesList.appendVarBytes(value);

        return new QueryResult(bytesList, new ArrayList<>());
    }

    @LdtpMethod(FIND_BY_KEY_CF)
    public QueryResult doFindByKeyCf(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);

        List<DbValue> values = lsmTree.find(key, columnFamily);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(DB_VALUE_LIST);
        values.forEach(bytesList::append);

        return new QueryResult(bytesList, new ArrayList<>());
    }

    @LdtpMethod(INSERT)
    public QueryResult doInsert(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);

        lsmTree.insert(key, columnFamily, column, value);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        List<AffectValue> affectValues = new ArrayList<>();
        AffectKey affectKey = new AffectKey(key, columnFamily, column);
        affectValues.add(new AffectValue(affectKey, value));

        return new QueryResult(bytesList, affectValues);
    }

    @LdtpMethod(DELETE)
    public QueryResult doDelete(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);

        lsmTree.delete(key, columnFamily, column);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        List<AffectValue> affectValues = new ArrayList<>();
        AffectKey affectKey = new AffectKey(key, columnFamily, column);
        affectValues.add(new AffectValue(affectKey, null));

        return new QueryResult(bytesList, affectValues);
    }
}
