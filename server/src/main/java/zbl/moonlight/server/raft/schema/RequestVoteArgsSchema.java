package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        /* 任期 */
        @SchemaEntry(name = RaftSchemaEntryName.TERM, type = SchemaEntryType.INT, order = 0),
        @SchemaEntry(name = RaftSchemaEntryName.LAST_LOG_INDEX, type = SchemaEntryType.INT, order = 1),
        @SchemaEntry(name = RaftSchemaEntryName.LAST_LOG_TERM, type = SchemaEntryType.INT, order = 2),
})
public interface RequestVoteArgsSchema extends MSerializable, Parsable {
}
