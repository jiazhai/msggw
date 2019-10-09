package org.apache.pulsar.proxy.server;

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

import org.eclipse.jetty.proxy.ProxyServlet;

/**
 * AdminProxyHandler is package private, so we need to get around that protection.
 */
public class AdminProxyHandlerCreator {
    public static ProxyServlet create(ProxyConfiguration config) {
        return new AdminProxyHandler(config, null);
    }
}
