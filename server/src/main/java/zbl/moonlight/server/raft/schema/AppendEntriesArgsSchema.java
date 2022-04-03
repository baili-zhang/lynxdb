package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        @SchemaEntry(name = AppendEntriesArgsSchema.TERM, type = SchemaEntryType.INT, order = 0),
        @SchemaEntry(name = AppendEntriesArgsSchema.PREV_LOG_INDEX, type = SchemaEntryType.INT, order = 1),
        @SchemaEntry(name = AppendEntriesArgsSchema.PREV_LOG_TERM, type = SchemaEntryType.INT, order = 2),
        @SchemaEntry(name = AppendEntriesArgsSchema.ENTRIES, type = SchemaEntryType.STRING, order = 3),
        @SchemaEntry(name = AppendEntriesArgsSchema.LEADER_COMMIT, type = SchemaEntryType.INT, order = 4),
})
public interface AppendEntriesArgsSchema extends MSerializable, Parsable {
    String TERM = "AppendEntriesArgsSchema.TERM";
    String PREV_LOG_INDEX = "AppendEntriesArgsSchema.PREV_LOG_INDEX";
    String PREV_LOG_TERM = "AppendEntriesArgsSchema.PREV_LOG_TERM";
    String ENTRIES = "AppendEntriesArgsSchema.ENTRIES";
    String LEADER_COMMIT = "AppendEntriesArgsSchema.LEADER_COMMIT";
}
