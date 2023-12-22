/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.server;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Version;
import com.bailizhang.lynxdb.core.recorder.FlightDataRecorder;
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

    private final LynxDbServer server;

    LynxDbMainServer() throws IOException {
        logger.info("LynxDB Version: \"{}\".", Version.LYNXDB);

        Configuration config = Configuration.getInstance();
        logger.info("Configuration: {}", config);

        G.I.converter(new Converter(config.charset()));
        FlightDataRecorder.enable(config.enableFlightRecorder());

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
