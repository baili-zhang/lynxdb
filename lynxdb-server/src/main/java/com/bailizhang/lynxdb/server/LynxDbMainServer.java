package com.bailizhang.lynxdb.server;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.server.mode.cluster.ClusterLynxDbServer;
import com.bailizhang.lynxdb.server.mode.single.SingleLynxDbServer;

import java.io.IOException;

import static com.bailizhang.lynxdb.server.context.Configuration.CLUSTER;
import static com.bailizhang.lynxdb.server.context.Configuration.SINGLE;

public class LynxDbMainServer {
    private static final String VERSION = "2023.2.4-snapshot";

    private final LynxDbServer server;

    LynxDbMainServer() throws IOException {
        Configuration config = Configuration.getInstance();

        G.I.converter(new Converter(config.charset()));

        String runningMode = config.runningMode();
        switch (runningMode) {
            case SINGLE -> server = new SingleLynxDbServer();
            case CLUSTER -> server = new ClusterLynxDbServer();

            default -> throw new RuntimeException("Undefined running mode: " + runningMode);
        }
    }

    public void run() {
        server.run();
    }

    public static void main(String[] args) throws IOException {
        new LynxDbMainServer().run();
    }
}
