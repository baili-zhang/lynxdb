package zbl.moonlight.server.raft.log;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        @SchemaEntry(name = RaftDataLogSchema.METHOD, type = SchemaEntryType.BYTE, order = 0),
        @SchemaEntry(name = RaftDataLogSchema.KEY, type = SchemaEntryType.STRING, order = 1),
        @SchemaEntry(name = RaftDataLogSchema.VALUE, type = SchemaEntryType.STRING, order = 2)
})
public interface RaftDataLogSchema extends MSerializable, Parsable {
    String METHOD = "RaftLogSchema.METHOD";
    String KEY = "RaftLogSchema.KEY";
    String VALUE = "RaftLogSchema.VALUE";
}
