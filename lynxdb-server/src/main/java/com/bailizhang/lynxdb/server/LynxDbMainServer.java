package com.bailizhang.lynxdb.server;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.server.mode.cluster.ClusterLynxDbServer;
import com.bailizhang.lynxdb.server.mode.single.SingleLynxDbServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.bailizhang.lynxdb.server.context.Configuration.Default.CLUSTER;
import static com.bailizhang.lynxdb.server.context.Configuration.Default.SINGLE;

public class LynxDbMainServer {
    private static final Logger logger = LoggerFactory.getLogger(LynxDbMainServer.class);

    private static final String VERSION = "2023.5.20-snapshot";

    private final LynxDbServer server;

    LynxDbMainServer() throws IOException {
        logger.info("LynxDB Version: \"{}\".", VERSION);

        Configuration config = Configuration.getInstance();
        logger.info("Configuration: {}", config);

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
