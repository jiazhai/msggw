package io.streaml.msggw.http;

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

import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import javax.servlet.AsyncContext;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.proxy.server.AdminProxyHandlerCreator;
import org.apache.pulsar.proxy.server.WebServer;
import org.apache.pulsar.websocket.WebSocketConsumerServlet;
import org.apache.pulsar.websocket.WebSocketProducerServlet;
import org.apache.pulsar.websocket.WebSocketReaderServlet;
import org.apache.pulsar.websocket.WebSocketService;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import io.streaml.msggw.MessagingGatewayConfiguration;

public class HttpService extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(HttpService.class);

    public static final String PULSAR_PRODUCER_THREAD_ATTRIBUTE = "pulsarProducerThread";
    private static final String PULSAR_HEADER_PREFIX = "X-Pulsar";
    public static final String PULSAR_HEADER_KEY = PULSAR_HEADER_PREFIX + "-Key";
    public static final String PULSAR_HEADER_PROPERTY = PULSAR_HEADER_PREFIX + "-Property";
    private final WebServer server;
    private final ExecutorService executor;
    private final Optional<WebSocketService> webSocketService;

    public HttpService(MessagingGatewayConfiguration config,
                       ExecutorService executor,
                       AuthenticationService authenticationService,
                       Optional<AuthorizationService> authorizationService,
                       HttpProducerThread producerThread) {
        this.executor = executor;
        server = new WebServer(config, authenticationService);

        if (config.isHttpDataApiEnabled()) {
            ServletHolder servletHolder = new ServletHolder(new TopicServlet(producerThread,
                                                                             authorizationService));
            servletHolder.setAsyncSupported(true);
            server.addServlet("/data/v1/topics", servletHolder);
        }

        if (config.isAdminApiEnabled()) {
            // must have broker serviceURL or broker webservice URL
            ServletHolder servletHolder = new ServletHolder(AdminProxyHandlerCreator.create(config));
            servletHolder.setInitParameter("preserveHost", "true");
            server.addServlet("/admin", servletHolder);
            server.addServlet("/lookup", servletHolder);
        }

        if (config.isMetricsEnabled()) {
            DefaultExports.initialize();
            server.addServlet("/metrics", new ServletHolder(MetricsServlet.class));
        }

        if (config.isVipStatusEnabled()) {
            server.addRestResources("/", VipStatus.class.getPackage().getName(),
                                    VipStatus.ATTRIBUTE_STATUS_FILE_PATH, config.getStatusFilePath());
        }

        if (config.isReverseProxyEnabled()) {
            for (MessagingGatewayConfiguration.HttpReverseProxyConfig revProxy : config.getHttpReverseProxyConfigs()) {
                log.debug("Adding reverse proxy with config {}", revProxy);
                ServletHolder proxyHolder = new ServletHolder(ProxyServlet.Transparent.class);
                proxyHolder.setInitParameter("proxyTo", revProxy.getProxyTo());
                proxyHolder.setInitParameter("prefix", "/");
                server.addServlet(revProxy.getPath(), proxyHolder);
            }
        }

        if (config.isWebSocketProxyEnabled()) {
            webSocketService = Optional.of(new WebSocketService(createClusterData(config),
                                                                config.toServiceConfiguration()));
            server.addServlet(WebSocketProducerServlet.SERVLET_PATH,
                              new ServletHolder("ws-events", new WebSocketProducerServlet(webSocketService.get())));
            server.addServlet(WebSocketConsumerServlet.SERVLET_PATH,
                              new ServletHolder("ws-events", new WebSocketConsumerServlet(webSocketService.get())));
            server.addServlet(WebSocketReaderServlet.SERVLET_PATH,
                              new ServletHolder("ws-events", new WebSocketReaderServlet(webSocketService.get())));


            server.addServlet(WebSocketProducerServlet.SERVLET_PATH_V2,
                              new ServletHolder("ws-events", new WebSocketProducerServlet(webSocketService.get())));
            server.addServlet(WebSocketConsumerServlet.SERVLET_PATH_V2,
                              new ServletHolder("ws-events", new WebSocketConsumerServlet(webSocketService.get())));
            server.addServlet(WebSocketReaderServlet.SERVLET_PATH_V2,
                              new ServletHolder("ws-events", new WebSocketReaderServlet(webSocketService.get())));

        } else {
            webSocketService = Optional.empty();
        }
    }

    @Override
    protected void doStart() {
        executor.submit(() -> {
                try {
                    if (webSocketService.isPresent()) {
                        webSocketService.get().start();
                    }
                    server.start();
                    notifyStarted();
                } catch (Exception e) {
                    log.error("Starting webserver threw exception");
                    notifyFailed(e);
                }
            });
    }

    @Override
    protected void doStop() {
        executor.submit(() -> {
                try {
                    server.stop();
                    if (webSocketService.isPresent()) {
                        webSocketService.get().close();
                    }
                    notifyStopped();
                } catch (Exception e) {
                    log.error("Starting webserver threw exception");
                    notifyFailed(e);
                }
            });
    }

    public URI getServiceUri() throws URISyntaxException {
        return server.getServiceUri();
    }

    private static ClusterData createClusterData(MessagingGatewayConfiguration config) {
        String brokerServiceURL = config.getBrokerServiceURL();
        String brokerServiceURLTLS = config.getBrokerServiceURLTLS();
        String brokerWebServiceURL = config.getBrokerWebServiceURL();
        String brokerWebServiceURLTLS = config.getBrokerWebServiceURLTLS();

        if (!isNullOrEmpty(brokerServiceURL) || !isNullOrEmpty(brokerServiceURLTLS)) {
            return new ClusterData(brokerWebServiceURL, brokerWebServiceURLTLS,
                                   brokerServiceURL, brokerServiceURLTLS);
        } else if (!isNullOrEmpty(brokerWebServiceURL) || !isNullOrEmpty(brokerWebServiceURLTLS)) {
            return new ClusterData(brokerWebServiceURL, brokerWebServiceURLTLS);
        } else {
            return null;
        }
    }
}
