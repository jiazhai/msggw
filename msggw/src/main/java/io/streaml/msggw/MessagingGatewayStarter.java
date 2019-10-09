package io.streaml.msggw;

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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.util.Timer;
import io.netty.util.HashedWheelTimer;

import io.streaml.conhash.CHashGroupService;
import io.streaml.conhash.CHashGroupZKImpl;
import io.streaml.msggw.binary.BinaryProxyService;

import io.streaml.msggw.http.HttpProducerThread;
import io.streaml.msggw.http.HttpService;

import io.streaml.msggw.kafka.ConsumerGroups;
import io.streaml.msggw.kafka.ConsumerGroupsImpl;
import io.streaml.msggw.kafka.Fetcher;
import io.streaml.msggw.kafka.FetcherImpl;
import io.streaml.msggw.kafka.KafkaProducerThread;
import io.streaml.msggw.kafka.KafkaRequestHandler;
import io.streaml.msggw.kafka.KafkaService;
import io.streaml.msggw.kafka.MLTableConsumerGroupStorageService;
import io.streaml.msggw.kafka.NodeIds;
import io.streaml.msggw.kafka.NodeIdsImpl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingGatewayStarter {
    private final static String VERSION_RESOURCE_NAME = "messaging-gateway-version.properties";
    private final static String GIT_RESOURCE_NAME = "messaging-gateway-git.properties";
    private final static Logger log = LoggerFactory.getLogger(MessagingGatewayStarter.class);

    private static class StarterArguments {
        @Parameter(names = {"-c", "--msggw-conf"}, description = "Configuration file for messaging gateway")
        private String configFile;

        @Parameter(names = {"-a", "--advertized-address"},
                   description = "Address (hostname or IP) to advertize as, must be routable by connecting clients")
        private String advertizedAddress;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    private static PulsarAdmin buildPulsarAdmin(MessagingGatewayConfiguration config)
            throws Exception {
        PulsarAdminBuilder builder = PulsarAdmin.builder().serviceHttpUrl(config.getBrokerWebServiceURL());
        if (!isBlank(config.getBrokerClientAuthenticationPlugin())) {
            builder.authentication(config.getBrokerClientAuthenticationPlugin(),
                                   config.getBrokerClientAuthenticationParameters());
        }
        return builder.build();
    }

    private static PulsarClient buildPulsarClient(MessagingGatewayConfiguration config)
            throws Exception {
        ClientBuilder builder = PulsarClient.builder().serviceUrl(config.getBrokerServiceURL());
        if (!isBlank(config.getBrokerClientAuthenticationPlugin())) {
            builder.authentication(config.getBrokerClientAuthenticationPlugin(),
                                   config.getBrokerClientAuthenticationParameters());
        }
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        StarterArguments starterArguments = new StarterArguments();
        JCommander jcommander = new JCommander(starterArguments);
        jcommander.setProgramName("MessagingGatewayStarter");
        jcommander.parse(args);

        if (starterArguments.help) {
            jcommander.usage();
            System.exit(-1);
        }

        // init broker config
        MessagingGatewayConfiguration config;
        if (isBlank(starterArguments.configFile)) {
            log.info("--msggw-conf not set, searching for msggw.conf on classpath");
            config = MessagingGatewayConfiguration.loadFromClasspath();
        } else {
            config = MessagingGatewayConfiguration.loadFromFile(starterArguments.configFile);
        }

        log.info("Starting Streamlio Messaging gateway; version: '{}'", getNormalizedVersionString());
        log.info("Git Revision {}", getGitSha());
        log.info("Built by {} on {} at {}", getBuildUser(), getBuildHost(), getBuildTime());


        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("msggw").setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.error("Caught exception in thread {}", t, e);
                        }).build());

        Timer timer = new HashedWheelTimer();
        List<Service> services = new ArrayList<>();
        ZooKeeperClientFactory zkClientFactory = null;
        OrderedExecutor orderedExecutor = OrderedExecutor.newBuilder().name("msggw").numThreads(1).build();
        GlobalZooKeeperCache globalZkCache = null;
        AuthorizationService authorizationService = null;
        String advertizedAddress = starterArguments.advertizedAddress;
        if (isBlank(advertizedAddress)) {
            advertizedAddress = config.getAdvertizedAddress();
        }
        zkClientFactory = new ZookeeperClientFactoryImpl();

        ZooKeeper zk = zkClientFactory.create(config.getZookeeperServers(),
                                              ZooKeeperClientFactory.SessionType.ReadWrite,
                                              (int) config.getZookeeperSessionTimeoutMs()).join();
        try (PulsarAdmin pulsarAdmin = buildPulsarAdmin(config);
             PulsarClient pulsarClient = buildPulsarClient(config)) {

            if (config.isAuthorizationEnabled()) {
                if (isBlank(config.getConfigurationStoreServers())) {
                    throw new IllegalArgumentException(
                            "Configuration store servers must be configured if authorization is enabled");
                }


                globalZkCache = new GlobalZooKeeperCache(zkClientFactory,
                        (int) config.getZookeeperSessionTimeoutMs(),
                        (int) TimeUnit.MILLISECONDS.toSeconds(config.getZookeeperSessionTimeoutMs()),
                        config.getConfigurationStoreServers(), orderedExecutor, scheduler);
                globalZkCache.start();

                authorizationService = new AuthorizationService(config.toServiceConfiguration(),
                                                                new ConfigurationCacheService(globalZkCache));

            }
            AuthenticationService authenticationService = new AuthenticationService(config.toServiceConfiguration());
            if (config.isKafkaEnabled()) {
                String advertizedAddressAndPort = advertizedAddress + ":" + config.getKafkaPort();
                log.info("Starting kafka on port {}", advertizedAddressAndPort);
                NodeIds nodeIds = new NodeIdsImpl(scheduler, zk, "/kafkagw-nodes");
                Fetcher fetcher = new FetcherImpl(pulsarClient, timer);
                CHashGroupService chashGroup = new CHashGroupZKImpl(
                        advertizedAddressAndPort,
                        "/kafkagw", zk,
                        orderedExecutor.chooseThread(0),
                        (e) -> {
                            log.error("FATAL: error in in consistent hash group, bailing", e);
                            System.exit(300);
                        });
                services.add(chashGroup);
                MLTableConsumerGroupStorageService storage = new MLTableConsumerGroupStorageService(
                        config.getZookeeperServers());
                services.add(storage);
                ConsumerGroups groups = new ConsumerGroupsImpl(chashGroup,
                                                               Ticker.systemTicker(),
                                                               storage,
                                                               orderedExecutor.chooseThread(0));
                KafkaProducerThread kafkaProducerThread = new KafkaProducerThread(pulsarClient);
                KafkaService kafka = new KafkaService(config.getKafkaPort(),
                                                      scheduler, groups,
                                                      new KafkaRequestHandler(config.getKafkaPulsarClusterName(),
                                                                              nodeIds,
                                                                              chashGroup,
                                                                              pulsarAdmin, pulsarClient,
                                                                              scheduler, kafkaProducerThread,
                                                                              fetcher, groups,
                                                                              authenticationService.getAuthenticationProvider("token"),
                                                                              Optional.ofNullable(authorizationService)));
                services.add(kafkaProducerThread);
                services.add(kafka);
            }

            if (config.isHttpEnabled()) {
                log.info("Starting http on port {}", config.getWebServicePort());
                HttpProducerThread httpProducerThread = new HttpProducerThread(pulsarClient);
                HttpService httpService = new HttpService(config, scheduler,
                                                          authenticationService,
                                                          Optional.ofNullable(authorizationService),
                                                          httpProducerThread);
                services.add(httpProducerThread);
                services.add(httpService);
            }

            if (config.isBinaryProtocolProxyEnabled()) {
                log.info("Starting binary proxy service on port {}", config.getServicePort());

                services.add(new BinaryProxyService(config, scheduler, authenticationService));
            }

            services.forEach(Service::startAsync);
            services.forEach(Service::awaitRunning);

            while (services.stream().allMatch(Service::isRunning)) {
                Thread.sleep(100);
            }
        } catch (Throwable t) {
            exitCode = -1;
            log.error("Service threw exception", t);
        } finally {
            services.stream().filter(Service::isRunning).forEach(Service::stopAsync);
            services.stream().filter(Service::isRunning).forEach(Service::awaitTerminated);

            if (globalZkCache != null) {
                globalZkCache.close();
            }
            if (orderedExecutor != null) {
                orderedExecutor.shutdown();
            }
            timer.stop();
            scheduler.shutdown();

            zk.close();
        }
        System.exit(exitCode);
    }

    private static String getGitSha() {
        String commit = getPropertyFromResource(GIT_RESOURCE_NAME, "git.commit.id");
        String dirtyString = getPropertyFromResource(GIT_RESOURCE_NAME, "git.dirty");
        if (dirtyString == null || Boolean.valueOf(dirtyString)) {
            return commit + "(dirty)";
        } else {
            return commit;
        }
    }

    private static String getBuildUser() {
        String email = getPropertyFromResource(GIT_RESOURCE_NAME, "git.build.user.email");
        String name = getPropertyFromResource(GIT_RESOURCE_NAME, "git.build.user.name");
        return String.format("%s <%s>", name, email);
    }

    private static String getBuildHost() {
        return getPropertyFromResource(GIT_RESOURCE_NAME, "git.build.host");
    }

    private static String getBuildTime() {
        return getPropertyFromResource(GIT_RESOURCE_NAME, "git.build.time");
    }

    private static String getPropertyFromResource(String resource, String propertyName) {
        try {
            InputStream stream = MessagingGatewayStarter.class.getClassLoader().getResourceAsStream(resource);
            if (stream == null) {
                return null;
            }
            Properties properties = new Properties();
            try {
                properties.load(stream);
                String propertyValue = (String) properties.get(propertyName);
                return propertyValue;
            } catch (IOException e) {
                return null;
            } finally {
                stream.close();
            }
        } catch (Throwable t) {
            return null;
        }
    }

    private static String getNormalizedVersionString() {
        return getPropertyFromResource(VERSION_RESOURCE_NAME, "version");
    }

}
