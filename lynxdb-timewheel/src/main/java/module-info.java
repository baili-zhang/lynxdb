module lynxdb.timewheel {
    requires static lynxdb.core;

    requires org.slf4j;

    exports com.bailizhang.lynxdb.timewheel;
    exports com.bailizhang.lynxdb.timewheel.task;
    exports com.bailizhang.lynxdb.timewheel.types;
}