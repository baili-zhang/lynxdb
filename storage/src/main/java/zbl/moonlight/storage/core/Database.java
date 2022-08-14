package zbl.moonlight.storage.core;

import zbl.moonlight.storage.rocks.query.Query;

import java.nio.file.Path;

public interface Database extends AutoCloseable {
    ResultSet<?> doQuery(Query<?, ?> query) throws Exception;
}
