package zbl.moonlight.client.mql;

import org.junit.jupiter.api.Test;
import zbl.moonlight.client.exception.SyntaxException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static zbl.moonlight.client.mql.MQL.Keywords.*;

class MQLTest {

    @Test
    void test_001() {
        String statement = """
                create kvstore table name1, ` table name2`; 
                """;

        assertThrowsExactly(SyntaxException.class, () -> MQL.parse(statement));
    }

    @Test
    void test_002_create_table() {
        String statement = """
                create kvstore `kv_store_name`;
                create table table_name1, table_name2;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).type().equals(KVSTORE);
        assert queries.get(1).type().equals(MQL.Keywords.TABLE);
    }

    @Test
    void test_003() {
        String statement = """
                show tables;
                 show  kvstores   ;
                show columns in `user_table`;
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
                SELECT `column1`, `column2`, `column3` FROM TABLE `table_name` WHERE KEY IN `key1`, `key2`, `key3`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).from().equals(MQL.Keywords.TABLE);
        assert queries.get(0).columns().size() == 3;
        assert queries.get(0).keys().size() == 3;
    }

    @Test
    void test_005_insert_into_table() {
        String statement = """
                INSERT INTO TABLE `table_name`
                      (`column1`,`column2`,`column3`)
                      VALUES
                          (`key1`, `value1_1`, `value1_2`, `value1_3`),
                          (`key2`, `value2_1`, `value2_2`, `value2_3`);
                          
                insert into table user (name, age) values (1, trump, 18);
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).rows().size() == 2;
        assert queries.get(0).tables().size() == 1;
        assert queries.get(0).columns().size() == 3;
    }

    @Test
    void test_006_create_columns() {
        String statement = """
                create columns `column_name1`, `column_name2` in `table_name`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).columns().size() == 2;
    }

    @Test
    void test_007_drop_table() {
        String statement = """
                DROP TABLE `user_table`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert DROP.equalsIgnoreCase(queries.get(0).name());
        assert TABLE.equalsIgnoreCase(queries.get(0).type());
        assert "user_table".equals(queries.get(0).tables().get(0));
    }

    @Test
    void test_007_drop_kvstore() {
        String statement = """
                DROP KVSTORE `user_kv`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert DROP.equalsIgnoreCase(queries.get(0).name());
        assert KVSTORE.equalsIgnoreCase(queries.get(0).type());
        assert "user_kv".equals(queries.get(0).kvstores().get(0));
    }

    @Test
    void test_007_drop_column() {
        String statement = """
                DROP COLUMNS `name`, `age` IN `user_table`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert DROP.equalsIgnoreCase(queries.get(0).name());
        assert COLUMNS.equalsIgnoreCase(queries.get(0).type());
        assert "user_table".equals(queries.get(0).tables().get(0));
        assert queries.get(0).columns().size() == 2;
    }

    @Test
    void test_008_select_from_kvstores() {
        String statement = """
                SELECT FROM KVSTORE `user_kv`
                    WHERE KEY IN `NO.1`, `NO.2`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).from().equalsIgnoreCase(KVSTORE);
        assert queries.get(0).columns().size() == 0;
        assert queries.get(0).keys().size() == 2;
    }

    @Test
    void test_008_insert_into_kvstores() {
        String statement = """
                INSERT INTO KVSTORE `kv_store_name`
                    VALUES
                        (`key`,`value`),
                        (`key`,`value`),
                        (`key`,`value`);
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).type().equalsIgnoreCase(KVSTORE);
        assert queries.get(0).columns().size() == 0;
        assert queries.get(0).rows().size() == 3;
    }

    @Test
    void test_008_delete_from_kvstores() {
        String statement = """
                DELETE `article_count`,`user_count` FROM KVSTORE `count_kv`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).from().equalsIgnoreCase(KVSTORE);
        assert queries.get(0).columns().size() == 0;
        assert queries.get(0).keys().size() == 2;
    }

    @Test
    void test_008_delete_from_table() {
        String statement = """
                DELETE `NO.1`, `NO.2` FROM TABLE `user_table`;
                """;

        List<MqlQuery> queries = MQL.parse(statement);

        assert queries.get(0).from().equalsIgnoreCase(TABLE);
        assert queries.get(0).columns().size() == 0;
        assert queries.get(0).keys().size() == 2;
    }
}