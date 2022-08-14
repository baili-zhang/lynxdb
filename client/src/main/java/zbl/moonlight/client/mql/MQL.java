package zbl.moonlight.client.mql;

import zbl.moonlight.client.exception.SyntaxException;

import java.util.ArrayList;
import java.util.List;

public interface MQL {
    interface Keywords {
        String CREATE       = "create";
        String DROP         = "drop";
        String INSERT       = "insert";
        String SELECT       = "select";
        String DELETE       = "delete";

        String COLUMN       = "column";

        String EXIT         = "exit";
        String CONNECT      = "connect";
        String DISCONNECT   = "disconnect";
        String CLUSTER      = "cluster";

        String TABLE        = "table";
        String KVSTORE      = "kvstore";
    }

    static List<MqlQuery> parse(String statement) {
        List<MqlQuery> mqlQueries = new ArrayList<>();

        char[] chs = statement.toCharArray();
        int curr = 0;

        while (curr < chs.length) {
            MqlQuery query = new MqlQuery();
            curr = parseSpace(chs, curr);

            if(curr >= chs.length) {
                break;
            }

            curr = parseQuery(chs, curr, query);
            mqlQueries.add(query);
        }

        return mqlQueries;
    }

    private static int parseQuery(char[] chs, int curr, MqlQuery query) {
        curr = parseStatement(chs, curr, query);
        return curr;
    }

    private static int parseSpace(char[] chs, int curr) {
        while(curr < chs.length && Character.isWhitespace(chs[curr])) {
            curr ++;
        }

        return curr;
    }

    private static int parseStatement(char[] chs, int curr, MqlQuery query) {
        StringBuilder str = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            str.append(chs[curr]);
            curr ++;
        }

        String command = str.toString().toLowerCase();

        return switch (command) {
            case Keywords.CREATE -> parseCreate(chs, curr, query);
            case Keywords.DELETE -> parseDelete(chs, curr, query);
            case Keywords.DROP -> parseDrop(chs, curr, query);
            case Keywords.SELECT -> parseSelect(chs, curr, query);
            case Keywords.INSERT -> parseInsert(chs, curr, query);

            case Keywords.CONNECT -> parseConnect(chs, curr, query);
            case Keywords.DISCONNECT -> parseDisconnect(chs, curr, query);
            case Keywords.EXIT -> parseExit(chs, curr, query);
            case Keywords.CLUSTER -> parseCluster(chs, curr, query);

            default -> throw new RuntimeException("Command [" + command + "] is not Support.");
        };
    }

    private static int parseCluster(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.CLUSTER);

        return curr;
    }

    private static int parseExit(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.EXIT);

        return curr;
    }

    private static int parseDisconnect(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.DISCONNECT);

        return curr;
    }

    private static int parseConnect(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.CONNECT);

        return curr;
    }

    private static int parseInsert(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.INSERT);

        return curr;
    }

    private static int parseSelect(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.SELECT);

        return curr;
    }

    private static int parseDrop(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.DROP);

        return curr;
    }

    private static int parseDelete(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.DELETE);

        return curr;
    }

    private static int parseCreate(char[] chs, int curr, MqlQuery query) {
        query.name(Keywords.CREATE);

        curr = parseSpace(chs, curr);
        curr = parseType(chs, curr, query);

        switch (query.type()) {
            case Keywords.TABLE, Keywords.KVSTORE -> {
                curr = parseSpace(chs, curr);
                curr = parseList(chs, curr, query);
            }

            case Keywords.COLUMN -> {

            }
        }

        return parseSemicolon(chs, curr);
    }

    private static int parseSemicolon(char[] chs, int curr) {
        curr = parseSpace(chs, curr);

        if(chs[curr] != ';') {
            throw new RuntimeException("Can not find end char ';'.");
        }

        return ++ curr;
    }

    private static int parseType(char[] chs, int curr, MqlQuery query) {
        StringBuilder str = new StringBuilder();

        while(curr < chs.length && chs[curr] != ' ') {
            str.append(chs[curr]);
            curr ++;
        }

        query.type(str.toString().toLowerCase());

        return curr;
    }

    private static int parseList(char[] chs, int curr, MqlQuery query) {
        while(curr < chs.length && chs[curr] != ' ') {
            curr = parseItem(chs, curr, query);
            curr = parseSpace(chs, curr);

            if(chs[curr] == ';') {
                break;
            }

            curr = parseComma(chs, curr);
            curr = parseSpace(chs, curr);
        }

        return curr;
    }

    private static int parseComma(char[] chs, int curr) {
        if(chs[curr] != ',') {
            throw new SyntaxException(chs, curr);
        }

        return ++ curr;
    }

    private static int parseItem(char[] chs, int curr, MqlQuery query) {
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
                        || Character.isWhitespace(chs[curr])) {
                    break;
                }

                str.append(chs[curr]);
                curr ++;
            }
        }

        String name = str.toString();

        if(Keywords.TABLE.equals(query.type())) {
            query.tables().add(name);
        } else if(Keywords.KVSTORE.equals(query.type())) {
            query.kvstores().add(name);
        }

        return curr;
    }
}
