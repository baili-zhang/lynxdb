package zbl.moonlight.core.field;

import org.junit.jupiter.api.Test;

public class NullFieldTest {
    static class TestNullField {
        @NullField
        private Integer n;
    }

    @Test
    void test() {
        new TestNullField();
    }
}
