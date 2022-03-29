package zbl.moonlight.core.protocol.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SchemaEntry {
    /* 名称 */
    String name();
    /* 是否有内容长度 */
    SchemaEntryType type();
    /* 排序用 */
    int order();
}
