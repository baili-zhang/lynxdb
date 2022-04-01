package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        @SchemaEntry(name = RaftSchemaEntryName.TERM, type = SchemaEntryType.INT, order = 0),
        @SchemaEntry(name = RaftSchemaEntryName.PREV_LOG_INDEX, type = SchemaEntryType.INT, order = 1),
        @SchemaEntry(name = RaftSchemaEntryName.PREV_LOG_TERM, type = SchemaEntryType.INT, order = 2),
        @SchemaEntry(name = RaftSchemaEntryName.ENTRIES, type = SchemaEntryType.STRING, order = 3),
        @SchemaEntry(name = RaftSchemaEntryName.LEADER_COMMIT, type = SchemaEntryType.INT, order = 4),
})
public interface AppendEntriesArgsSchema extends MSerializable, Parsable {
}
