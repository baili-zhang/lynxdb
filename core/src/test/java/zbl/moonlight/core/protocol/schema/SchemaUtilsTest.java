package zbl.moonlight.core.protocol.schema;

import org.junit.jupiter.api.Test;
import java.util.List;

class SchemaUtilsTest {

    @Schema({
            @SchemaEntry(name = "SchemaParent", type = SchemaEntryType.BYTE, order = 0),
    })
    interface SchemaParent {
    }

    @Schema({
            @SchemaEntry(name = "SchemaChild", type = SchemaEntryType.BYTE, order = 1),
    })
    interface SchemaChild extends SchemaParent {

    }

    interface SchemaEmpty extends SchemaParent {

    }

    @Test
    void testListAll() {
        List<SchemaEntry> schemaEntries = SchemaUtils.listAll(SchemaChild.class);
        SchemaUtils.sort(schemaEntries);
        assert schemaEntries.size() == 2;
        assert schemaEntries.get(0).order() < schemaEntries.get(1).order();
    }

    @Test
    void testListAllEmpty() {
        List<SchemaEntry> schemaEntries = SchemaUtils.listAll(SchemaEmpty.class);
        assert schemaEntries.size() == 0;
    }
}