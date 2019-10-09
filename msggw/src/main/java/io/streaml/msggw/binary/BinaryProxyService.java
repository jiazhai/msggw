package io.streaml.msggw.binary;

/*
 * Copyright 2019 Streamlio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.util.concurrent.AbstractService;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import io.streaml.msggw.MessagingGatewayConfiguration;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.proxy.server.ProxyService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryProxyService extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(BinaryProxyService.class);

    private final ProxyService service;
    private final ExecutorService executor;

    public BinaryProxyService(MessagingGatewayConfiguration config,
                              ExecutorService executor,
                              AuthenticationService authService) throws IOException {
        this.service = new ProxyService(config, authService);
        this.executor = executor;
    }

    @Override
    protected void doStart() {
        executor.submit(() -> {
                try {
                    service.start();
                    notifyStarted();
                } catch (Exception e) {
                    log.error("Starting binary proxy threw exception");
                    notifyFailed(e);
                }
            });
    }

    @Override
    protected void doStop() {
        executor.submit(() -> {
                try {
                    service.close();
                    notifyStopped();
                } catch (Exception e) {
                    log.error("Starting binary proxy threw exception");
                    notifyFailed(e);
                }
            });
    }

}

