package zbl.moonlight.server.storage.core;

import zbl.moonlight.server.storage.query.CfDeleteQuery;
import zbl.moonlight.server.storage.query.CfGetQuery;
import zbl.moonlight.server.storage.query.CfSetQuery;
import zbl.moonlight.server.storage.query.ResultSet;

public interface ColumnFamilyStorable extends BaseStorable {
    ResultSet cfGet(CfGetQuery query);
    ResultSet cfSet(CfSetQuery query);
    ResultSet cfDelete(CfDeleteQuery query);
}
