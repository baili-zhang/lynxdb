module test.lynxdb.lsmtree {
    requires lynxdb.core;
    requires lynxdb.lsmtree;
    requires org.junit.jupiter.api;

    opens com.bailizhang.test.lynxdb.lsmtree to org.junit.platform.commons;
}