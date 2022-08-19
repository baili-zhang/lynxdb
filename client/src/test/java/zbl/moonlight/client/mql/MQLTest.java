package zbl.moonlight.client.mql;

import org.junit.jupiter.api.Test;
import zbl.moonlight.client.exception.SyntaxException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MQLTest {

    @Test
    void test_001() {
        String statement = """
                create kvstore table name1, ` table name2`; 
                """;
        assertThrowsExactly(SyntaxException.class, () -> MQL.parse(statement));
    }

    @Test
    void test_002() {
        String statement = """
                create kvstore `kv_store_name`;
                create table table_name1, table_name2;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).type().equals(MQL.Keywords.KVSTORE);
        assert queries.get(1).type().equals(MQL.Keywords.TABLE);
    }

    @Test
    void test_003() {
        String statement = """
                show tables;
                 show  kvstores   ;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).name().equals(MQL.Keywords.SHOW);
        assert queries.get(0).type().equals(MQL.Keywords.TABLES);
        assert queries.get(1).name().equals(MQL.Keywords.SHOW);
        assert queries.get(1).type().equals(MQL.Keywords.KVSTORES);
    }

    @Test
    void test_004_select_from_table () {
        String statement = """
                select `column1`, `column2`, `column3` from table `table_name` where key in `key1`, `key2`, `key3`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).from().equals(MQL.Keywords.TABLE);
        assert queries.get(0).columns().size() == 3;
        assert queries.get(0).keys().size() == 3;
    }
}