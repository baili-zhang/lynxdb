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

package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;

@Configuration
@ConditionalOnClass(LynxDbConnection.class)
@EnableConfigurationProperties(LynxDbProperties.class)
public class LynxDbAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(LynxDbAutoConfiguration.class);

    private final LynxDbProperties properties;

    public LynxDbAutoConfiguration(LynxDbProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean
    LynxDbClient lynxDbClient() {
        LynxDbClient lynxDbClient = new LynxDbClient();
        lynxDbClient.start();

        return lynxDbClient;
    }

    @Bean
    @ConditionalOnBean(LynxDbClient.class)
    @ConditionalOnMissingBean
    LynxDbConnection lynxDbConnection(LynxDbClient lynxDbClient) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));

        String host = properties.getHost();
        int port  = properties.getPort();

        ServerNode serverNode = new ServerNode(host, port);

        LynxDbConnection connection = lynxDbClient.createConnection(serverNode);

        try {
            connection.connect();
        } catch (ConnectException e) {
            logger.error(e.getMessage());
        }

        return connection;
    }
}
