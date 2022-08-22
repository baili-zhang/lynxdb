package zbl.moonlight.client.printer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class TablePrinterTest {

    private final List<List<String>> table = new ArrayList<>();

    @BeforeEach
    void setUp() {
        for (int i = 0; i < 10; i++) {
            List<String> row = new ArrayList<>();

            for (int j = 0; j < 6; j++) {
                row.add(randomStr(3, 20));
            }

            table.add(row);
        }
    }

    @Test
    void test_001_print() {
        new TablePrinter(table).print();
    }

    private String randomStr(int minLength, int maxLength) {

        Random random = new Random();
        int length = (int) (maxLength * random.nextDouble());
        length = Math.max(length, minLength);

        char[] charArray = new char[length];

        for (int i = 0; i < length; i ++) {
            int tempInt = 32 + (int) (94 * random.nextDouble());
            charArray[i] = (char) (tempInt);
        }

        return new String(charArray);
    }
}