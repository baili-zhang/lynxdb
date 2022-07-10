package zbl.moonlight.storage.query.cf;

import zbl.moonlight.storage.core.Queryable;

import java.util.List;

public interface CfQueryable extends Queryable {
    List<byte[]> columnFamilies();
}
