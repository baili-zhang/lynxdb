module lynxdb.spring.boot.starter {
    requires lynxdb.core;
    requires lynxdb.client;
    requires lynxdb.socket;

    requires spring.boot;
    requires spring.boot.autoconfigure;
    requires spring.context;
    requires spring.beans;
}