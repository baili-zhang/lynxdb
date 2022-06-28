package zbl.moonlight.server.storage.core;

import zbl.moonlight.server.storage.query.KvDeleteQuery;
import zbl.moonlight.server.storage.query.KvGetQuery;
import zbl.moonlight.server.storage.query.KvSetQuery;
import zbl.moonlight.server.storage.query.ResultSet;

public interface KeyValueStorable extends BaseStorable {
    ResultSet kvGet(KvGetQuery query);
    ResultSet kvSet(KvSetQuery query);
    ResultSet kvDelete(KvDeleteQuery query);
}
