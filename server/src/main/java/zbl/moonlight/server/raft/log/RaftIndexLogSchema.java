package zbl.moonlight.server.raft.log;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        @SchemaEntry(name = RaftIndexLogSchema.TERM, type = SchemaEntryType.BYTE, order = 0),
        @SchemaEntry(name = RaftIndexLogSchema.COMMIT_INDEX, type = SchemaEntryType.STRING, order = 1),
        @SchemaEntry(name = RaftIndexLogSchema.OFFSET, type = SchemaEntryType.STRING, order = 2),
        @SchemaEntry(name = RaftIndexLogSchema.LENGTH, type = SchemaEntryType.STRING, order = 3)
})
public interface RaftIndexLogSchema extends MSerializable, Parsable {
    String TERM = "RaftIndexLogSchema.TERM";
    String COMMIT_INDEX = "RaftIndexLogSchema.COMMIT_INDEX";
    String OFFSET = "RaftIndexLogSchema.OFFSET";
    String LENGTH = "RaftIndexLogSchema.LENGTH";
}
