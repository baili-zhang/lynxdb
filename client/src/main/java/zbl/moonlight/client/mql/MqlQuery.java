package zbl.moonlight.client.mql;

import java.util.ArrayList;
import java.util.List;

public class MqlQuery {
    private String name;
    private String type;
    private List<String> kvstores = new ArrayList<>();
    private List<String> tables = new ArrayList<>();

    public void name(String val) {
        name = val;
    }

    public String name() {
        return name;
    }

    public void type(String val) {
        type = val;
    }

    public String type() {
        return type;
    }

    public void kvstores(List<String> val) {
        kvstores = val;
    }

    public List<String> kvstores() {
        return kvstores;
    }

    public void tables(List<String> val) {
        tables = val;
    }

    public List<String> tables() {
        return tables;
    }
}
