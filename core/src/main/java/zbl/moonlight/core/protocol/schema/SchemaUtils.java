package zbl.moonlight.core.protocol.schema;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SchemaUtils {
    public static List<SchemaEntry> listAll(Class<?> clazz) {
        Schema schema = clazz.getAnnotation(Schema.class);
        if(schema == null) return new ArrayList<>();

        List<SchemaEntry> schemaEntries = new ArrayList<>(Arrays.asList(schema.value()));

        Type[] types = clazz.getGenericInterfaces();
        for (Type type : types) {
            Class<?> clazzForType;

            try {
                clazzForType = Class.forName(type.getTypeName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                continue;
            }
            schemaEntries.addAll(listAll(clazzForType));
        }

        return schemaEntries;
    }

    public static void sort(List<SchemaEntry> schemaEntries) {
        schemaEntries.sort(Comparator.comparingInt(SchemaEntry::order));
    }
}
