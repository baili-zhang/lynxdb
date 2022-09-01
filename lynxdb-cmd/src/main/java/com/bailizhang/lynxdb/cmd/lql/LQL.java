package com.bailizhang.lynxdb.cmd.lql;

import com.bailizhang.lynxdb.cmd.exception.SyntaxException;

import java.util.ArrayList;
import java.util.List;

public interface LQL {
    interface Keywords {
        String CREATE       = "create";
        String DROP         = "drop";
        String INSERT       = "insert";
        String SELECT       = "select";
        String DELETE       = "delete";
        String SHOW         = "show";

        String COLUMN       = "column";

        String EXIT         = "exit";
        String CONNECT      = "connect";
        String DISCONNECT   = "disconnect";
        String CLUSTER      = "cluster";

        String TABLE        = "table";
        String KVSTORE      = "kvstore";

        String TABLES       = "tables";
        String KVSTORES     = "kvstores";
        String VALUES       = "values";
        String COLUMNS      = "columns";

        String FROM         = "from";
        String WHERE        = "where";
        String KEY          = "key";
        String IN           = "in";
        String INTO         = "into";
    }

    static List<LqlQuery> parse(String statement) {
        List<LqlQuery> mqlQueries = new ArrayList<>();

        char[] chs = statement.toCharArray();
        int curr = 0;

        while (curr < chs.length) {
            LqlQuery query = new LqlQuery();
            curr = parseSpace(chs, curr);

            if(curr >= chs.length) {
                break;
            }

            curr = parseQuery(chs, curr, query);
            mqlQueries.add(query);
        }

        return mqlQueries;
    }

    private static int parseQuery(char[] chs, int curr, LqlQuery query) {
        curr = parseStatement(chs, curr, query);
        return curr;
    }

    private static int parseSpace(char[] chs, int curr) {
        while(curr < chs.length && Character.isWhitespace(chs[curr])) {
            curr ++;
        }

        return curr;
    }

    private static int parseStatement(char[] chs, int curr, LqlQuery query) {
        StringBuilder str = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            str.append(chs[curr]);
            curr ++;
        }

        String command = str.toString();

        curr = parseSpace(chs, curr);

        return switch (command.toLowerCase()) {
            case Keywords.CREATE -> parseCreate(chs, curr, query);
            case Keywords.DELETE -> parseDelete(chs, curr, query);
            case Keywords.DROP -> parseDrop(chs, curr, query);
            case Keywords.SELECT -> parseSelect(chs, curr, query);
            case Keywords.INSERT -> parseInsert(chs, curr, query);
            case Keywords.SHOW -> parseShow(chs, curr, query);

            case Keywords.CONNECT -> parseConnect(chs, curr, query);
            case Keywords.DISCONNECT -> parseDisconnect(chs, curr, query);
            case Keywords.EXIT -> parseExit(chs, curr, query);
            case Keywords.CLUSTER -> parseCluster(chs, curr, query);

            default -> throw new RuntimeException("Command [" + command + "] is not Support.");
        };
    }

    private static int parseShow(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.SHOW);

        StringBuilder str = new StringBuilder();
        while(!Character.isWhitespace(chs[curr]) && chs[curr] != ';') {
            str.append(chs[curr ++]);
        }

        String type = str.toString();

        switch (type.toLowerCase()) {
            case Keywords.TABLES, Keywords.KVSTORES -> query.type(type);
            case Keywords.COLUMNS -> {
                query.type(type);
                curr = parseSpace(chs, curr);
                curr = parseIn(chs, curr);
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.tables());
            }

            default -> throw new SyntaxException(chs, curr);
        }

        curr = parseSpace(chs, curr);

        return parseSemicolon(chs, curr);
    }

    private static int parseCluster(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.CLUSTER);

        return curr;
    }

    private static int parseExit(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.EXIT);

        return curr;
    }

    private static int parseDisconnect(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.DISCONNECT);

        return curr;
    }

    private static int parseConnect(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.CONNECT);

        return curr;
    }

    private static int parseInsert(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.INSERT);

        curr = parseInto(chs, curr);
        curr = parseSpace(chs, curr);
        curr = parseType(chs, curr, query);
        curr = parseSpace(chs, curr);

        switch (query.type().toLowerCase()) {
            case Keywords.TABLE -> {
                curr = parseList(chs, curr, query.tables());
                curr = parseSpace(chs, curr);
                curr = parseVector(chs, curr, query.columns());
            }

            case Keywords.KVSTORE -> {
                curr = parseList(chs, curr, query.kvstores());
            }

            default -> throw new SyntaxException(chs, curr);
        }

        curr = parseSpace(chs, curr);
        curr = parseValues(chs, curr);
        curr = parseSpace(chs, curr);
        curr = parseVectors(chs, curr, query.rows());
        curr = parseSpace(chs, curr);

        return parseSemicolon(chs, curr);
    }

    static int parseValues(char[] chs, int curr) {
        StringBuilder values = new StringBuilder();

        while(curr < chs.length && !Character.isWhitespace(chs[curr])) {
            values.append(chs[curr]);
            curr ++;
        }

        if(!values.toString().equalsIgnoreCase(Keywords.VALUES)) {
            throw new SyntaxException(chs, curr);
        }

        return curr;
    }

    static int parseVectors(char[] chs, int curr,
                            List<List<String>> vectors) {

        while (curr < chs.length) {
            List<String> vector = new ArrayList<>();
            curr = parseVector(chs, curr, vector);
            vectors.add(vector);

            if(chs[curr] != ',') {
                break;
            } else {
                ++ curr;
            }

            curr = parseSpace(chs, curr);
        }

        return curr;
    }

    static int parseVector(char[] chs, int curr,
                           List<String> vector) {
        if(chs[curr] != '(') {
            throw new SyntaxException(chs, curr);
        } else {
            ++ curr;
        }

        curr = parseSpace(chs, curr);
        curr = parseList(chs, curr, vector);
        curr = parseSpace(chs, curr);

        if(chs[curr] != ')') {
            throw new SyntaxException(chs, curr);
        } else {
            ++ curr;
        }

        return parseSpace(chs, curr);
    }

    static int parseInto(char[] chs, int curr) {
        StringBuilder into = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            into.append(chs[curr]);
            curr ++;
        }

        if(!into.toString().equalsIgnoreCase(Keywords.INTO)) {
            throw new SyntaxException(chs, curr);
        }

        return curr;
    }

    private static int parseSelect(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.SELECT);

        try {
            curr = parseFrom(chs, curr, query);
            curr = parseSpace(chs, curr);

            curr = parseItem(chs, curr, query.kvstores());
        } catch (SyntaxException ignored) {
            curr = parseList(chs, curr, query.columns());
            curr = parseSpace(chs, curr);

            curr = parseFrom(chs, curr, query);
            curr = parseSpace(chs, curr);

            curr = parseItem(chs, curr, query.tables());
        }

        curr = parseSpace(chs, curr);

        curr = parseWhere(chs, curr, query);
        curr = parseSpace(chs, curr);

        return parseSemicolon(chs, curr);
    }

    static int parseWhere(char[] chs, int curr, LqlQuery query) {
        StringBuilder where = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            where.append(chs[curr]);
            curr ++;
        }

        if(!where.toString().equalsIgnoreCase(Keywords.WHERE)) {
            throw new SyntaxException(chs, curr);
        }

        curr = parseSpace(chs, curr);

        StringBuilder key = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            key.append(chs[curr]);
            curr ++;
        }

        if(!key.toString().equalsIgnoreCase(Keywords.KEY)) {
            throw new SyntaxException(chs, curr);
        }

        curr = parseSpace(chs, curr);
        curr = parseIn(chs, curr);
        curr = parseSpace(chs, curr);
        curr = parseList(chs, curr, query.keys());

        return curr;
    }

    static int parseIn(char[] chs, int curr) {
        StringBuilder in = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            in.append(chs[curr]);
            curr ++;
        }

        if(!in.toString().equalsIgnoreCase(Keywords.IN)) {
            throw new SyntaxException(chs, curr);
        }

        return curr;
    }


    static int parseFrom(char[] chs, int curr, LqlQuery query) {
        StringBuilder from = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            from.append(chs[curr]);
            curr ++;
        }

        if(!from.toString().equalsIgnoreCase(Keywords.FROM)) {
            throw new SyntaxException(chs, curr);
        }

        curr = parseSpace(chs, curr);

        StringBuilder type = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            type.append(chs[curr]);
            curr ++;
        }

        query.from(type.toString().toLowerCase());

        return curr;
    }

    private static int parseDrop(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.DROP);

        curr = parseType(chs, curr, query);

        switch (query.type().toLowerCase()) {
            case Keywords.KVSTORE -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.kvstores());
            }

            case Keywords.TABLE -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.tables());
            }

            case Keywords.COLUMNS -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.columns());
                curr = parseSpace(chs, curr);
                curr = parseIn(chs, curr);
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.tables());
            }
        }

        return parseSemicolon(chs, curr);
    }

    private static int parseDelete(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.DELETE);

        curr = parseList(chs, curr, query.keys());
        curr = parseSpace(chs, curr);
        curr = parseFrom(chs, curr, query);
        curr = parseSpace(chs, curr);

        switch (query.from()) {
            case Keywords.TABLE ->
                    curr = parseItem(chs, curr, query.tables());

            case Keywords.KVSTORE ->
                    curr = parseItem(chs, curr, query.kvstores());

            default -> throw new SyntaxException(chs, curr);
        }

        curr = parseSpace(chs, curr);

        return parseSemicolon(chs, curr);
    }

    private static int parseCreate(char[] chs, int curr, LqlQuery query) {
        query.name(Keywords.CREATE);

        curr = parseType(chs, curr, query);

        switch (query.type().toLowerCase()) {
            case Keywords.KVSTORE -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.kvstores());
            }

            case Keywords.TABLE -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.tables());
            }

            case Keywords.COLUMNS -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.columns());
                curr = parseSpace(chs, curr);
                curr = parseIn(chs, curr);
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query.tables());
            }
        }

        return parseSemicolon(chs, curr);
    }

    private static int parseSemicolon(char[] chs, int curr) {
        curr = parseSpace(chs, curr);

        if(chs[curr] != ';') {
            throw new SyntaxException(chs, curr);
        }

        return ++ curr;
    }

    private static int parseType(char[] chs, int curr, LqlQuery query) {
        StringBuilder str = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            str.append(chs[curr]);
            curr ++;
        }

        query.type(str.toString().toLowerCase());

        return curr;
    }

    private static int parseList(char[] chs, int curr, List<String> list) {
        while(curr < chs.length && chs[curr] != ' ') {
            curr = parseItem(chs, curr, list);
            curr = parseSpace(chs, curr);

            if(chs[curr] == ';') {
                break;
            }

            if(chs[curr] != ',') {
                break;
            } else {
                curr ++;
            }

            curr = parseSpace(chs, curr);
        }

        return curr;
    }

    private static int parseItem(char[] chs, int curr, List<String> list) {
        StringBuilder str = new StringBuilder();

        if(chs[curr] == '`') {
            curr ++;
            while(curr < chs.length && chs[curr] != '`') {
                str.append(chs[curr]);
                curr ++;
            }
            curr ++;
        } else {
            while(curr < chs.length) {
                if(chs[curr] == ';' || chs[curr] == ','
                        || Character.isWhitespace(chs[curr])
                        || chs[curr] == ')') {
                    break;
                }

                str.append(chs[curr]);
                curr ++;
            }
        }

        String name = str.toString();
        list.add(name);

        return curr;
    }
}
