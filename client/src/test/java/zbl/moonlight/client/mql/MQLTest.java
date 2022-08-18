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
}