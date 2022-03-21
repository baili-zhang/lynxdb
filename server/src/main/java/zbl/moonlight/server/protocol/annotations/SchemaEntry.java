package zbl.moonlight.server.protocol.annotations;

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
    boolean hasLengthSize();
    /* Entry的长度占多少byte（用于不定的内容长度，hasLength为true） */
    int lengthSize() default -1;
    /* 内容长度（用于固定内容长度，hasLength为false） */
    int length() default -1;
}
