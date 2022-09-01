package com.bailizhang.lynxdb.cmd.lql;

import java.util.ArrayList;
import java.util.List;

public class LqlQuery {
    private String name;
    private String type;
    private String from;
    private List<String> kvstores = new ArrayList<>();
    private List<String> tables = new ArrayList<>();
    private List<String> columns = new ArrayList<>();
    private List<String> keys = new ArrayList<>();
    private List<List<String>> rows = new ArrayList<>();

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

    public void from(String val) {
        from = val;
    }

    public String from() {
        return from;
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

    public void columns(List<String> val) {
        columns = val;
    }

    public List<String> columns() {
        return columns;
    }

    public void keys(List<String> val) {
        keys = val;
    }

    public List<String> keys() {
        return keys;
    }

    public void rows(List<List<String>> val) {
        rows = val;
    }

    public List<List<String>> rows() {
        return rows;
    }
}
