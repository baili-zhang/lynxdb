package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        /* 任期 */
        @SchemaEntry(name = RaftResultSchema.TERM, type = SchemaEntryType.INT, order = 0),
})
public interface RaftResultSchema extends MSerializable, Parsable {
    String TERM = "RaftResultSchema.TERM";
}
