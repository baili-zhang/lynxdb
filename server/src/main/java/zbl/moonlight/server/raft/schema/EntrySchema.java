package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        @SchemaEntry(name = EntrySchema.TERM, type = SchemaEntryType.INT, order = 0),
        @SchemaEntry(name = EntrySchema.COMMIT_INDEX, type = SchemaEntryType.INT, order = 1),
        @SchemaEntry(name = EntrySchema.METHOD, type = SchemaEntryType.BYTE, order = 2),
        @SchemaEntry(name = EntrySchema.KEY, type = SchemaEntryType.STRING, order = 3),
        @SchemaEntry(name = EntrySchema.VALUE, type = SchemaEntryType.STRING, order = 4)
})
public interface EntrySchema extends MSerializable, Parsable {
    String TERM = "EntrySchema.TERM";
    String COMMIT_INDEX = "EntrySchema.COMMIT_INDEX";
    String METHOD = "EntrySchema.METHOD";
    String KEY = "EntrySchema.KEY";
    String VALUE = "EntrySchema.VALUE";
}
