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

import static org.mockito.Mockito.spy;

import java.util.Optional;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;

import io.jsonwebtoken.SignatureAlgorithm;

import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.AuthAction;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.zookeeper.ZooKeeper;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.reflect.Field;
import java.net.BindException;
import java.net.URI;
import java.net.URL;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockPulsarCluster extends AbstractService {
    private final static Logger log = LoggerFactory.getLogger(MockPulsarCluster.class);

    public final static String SECRET_KEY = AuthTokenUtils.encodeKeyBase64(
            AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS512));
    public final static String ADMIN_TOKEN = userToken("admin");
    public final static String USER1_TOKEN = userToken("user1");
    public final static String USER2_TOKEN = userToken("user2");
    public final static String USER3_TOKEN = userToken("user3");

    private MockPulsarService service;

    @Override
    protected void doStart() {
        try {
            for (int i = 0; i < 100; i++) {
                try {
                    service = new MockPulsarService();
                    service.setup();
                    notifyStarted();
                    return;
                } catch (PulsarServerException pse) {
                    if (pse.getCause() instanceof BindException) {
                        // retry
                    } else {
                        throw pse;
                    }
                }
            }
            notifyFailed(new Exception("Unable to bind a port"));
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            service.cleanup();
            notifyStopped();
        } catch (Exception e) {
            notifyFailed(e);
        }
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        return service.getZooKeeperClientFactory();
    }

    public OrderedExecutor getExecutor() {
        return service.getExecutor();
    }

    public ZooKeeper getZooKeeperClient() {
        return service.getZooKeeperClient();
    }

    public PulsarClient createPulsarClient(String user) throws Exception {
        return PulsarClient.builder().serviceUrl(lookupUrl())
            .authentication(AuthenticationFactory.token(userToken(user)))
            .build();
    }

    public PulsarClient createSuperUserClient() throws Exception {
        return PulsarClient.builder().serviceUrl(lookupUrl())
            .authentication(AuthenticationFactory.token(ADMIN_TOKEN))
            .build();

    }

    public PulsarAdmin getPulsarAdmin() {
        return service.getPulsarAdmin();
    }

    public String httpServiceUrl() {
        return service.httpServiceUrl();
    }

    public String lookupUrl() {
        return service.lookupUrl();
    }

    public AuthenticationProvider getTokenAuthnProvider() {
        return service.getTokenAuthnProvider();
    }

    public AuthorizationService getAuthzService() {
        return service.getAuthzService();
    }

    public AuthenticationService getAuthService() {
        return service.getAuthService();
    }

    public ServiceConfiguration getServiceConfiguration() {
        return service.getServiceConfiguration();
    }

    public static String userToken(String username) {
        try {
            return AuthTokenUtils.createToken(
                    AuthTokenUtils.decodeSecretKey(AuthTokenUtils.readKeyFromUrl(MockPulsarCluster.SECRET_KEY)),
                    username, Optional.empty());
        } catch (Exception e) {
            log.error("Error generating token for {}", username, e);
            return "ERROR_TOKEN";
        }
    }

    static class MockPulsarService {
        protected ServiceConfiguration conf;
        protected PulsarService pulsar;
        protected PulsarAdmin admin;
        protected PulsarClient pulsarClient;
        protected URL brokerUrl;
        protected URI lookupUrl;

        protected MockZooKeeper mockZookKeeper;
        protected PulsarMockBookKeeper mockBookKeeper;
        protected boolean isTcpLookup = false;
        protected final String configClusterName = "test";

        private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
        private ExecutorService bkExecutor;

        int findFreePort() throws Exception {
            try (ServerSocket ss = new ServerSocket(0)) {
                ss.setReuseAddress(true);
                return ss.getLocalPort();
            }
        }

        MockPulsarService() throws Exception {
            this.conf = new ServiceConfiguration();
            this.conf.setAdvertisedAddress("localhost");
            this.conf.setBrokerServicePort(Optional.of(findFreePort()));
            this.conf.setAdvertisedAddress("localhost");
            this.conf.setWebServicePort(Optional.of(findFreePort()));
            this.conf.setClusterName(configClusterName);
            this.conf.setManagedLedgerCacheSizeMB(8);
            this.conf.setActiveConsumerFailoverDelayTimeMillis(0);
            this.conf.setDefaultNumberOfNamespaceBundles(1);
            this.conf.setZookeeperServers("localhost:2181");
            this.conf.setConfigurationStoreServers("localhost:3181");
            this.conf.setNumHttpServerThreads(16);

            this.conf.setAuthenticationEnabled(true);
            this.conf.setAuthorizationEnabled(true);
            this.conf.setSuperUserRoles(Sets.newHashSet("admin"));
            this.conf.setAuthenticationProviders(
                    Sets.newHashSet("org.apache.pulsar.broker.authentication.AuthenticationProviderToken"));
            this.conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationToken");
            this.conf.setBrokerClientAuthenticationParameters(ADMIN_TOKEN);

            this.conf.getProperties().setProperty("tokenSecretKey", SECRET_KEY);
        }

        String httpServiceUrl() {
            return brokerUrl.toString();
        }

        String lookupUrl() {
            return lookupUrl.toString();
        }

        ZooKeeper getZooKeeperClient() {
            return mockZookKeeper;
        }

        AuthenticationProvider getTokenAuthnProvider() {
            return pulsar.getBrokerService().getAuthenticationService().getAuthenticationProvider("token");
        }

        AuthorizationService getAuthzService() {
            return pulsar.getBrokerService().getAuthorizationService();
        }

        AuthenticationService getAuthService() {
            return pulsar.getBrokerService().getAuthenticationService();
        }

        ServiceConfiguration getServiceConfiguration() {
            return conf;
        }

        PulsarAdmin getPulsarAdmin() {
            return admin;
        }

        protected void setup() throws Exception {
            sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
            bkExecutor = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("mock-pulsar-bk")
                    .setUncaughtExceptionHandler((thread, ex) -> log.info("Uncaught exception", ex))
                    .build());

            mockZookKeeper = createMockZooKeeper();
            mockBookKeeper = new PulsarMockBookKeeper(mockZookKeeper, bkExecutor);

            this.pulsar = startBroker(this.conf);

            brokerUrl = new URL("http://localhost:" + conf.getWebServicePort().get());

            admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                        .authentication(AuthenticationFactory.token(ADMIN_TOKEN))
                        .build());

            lookupUrl = new URI(brokerUrl.toString());

            admin.clusters().createCluster(
                    "test", new ClusterData("http://127.0.0.1:" + conf.getWebServicePort().get()));
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
            admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));

            admin.namespaces().grantPermissionOnNamespace("public/default", "admin",
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
            admin.namespaces().createNamespace("public/user1", Sets.newHashSet("test"));
            admin.namespaces().grantPermissionOnNamespace("public/user1", "user1",
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
            admin.namespaces().createNamespace("public/user2", Sets.newHashSet("test"));
            admin.namespaces().grantPermissionOnNamespace("public/user2", "user2",
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
            admin.namespaces().createNamespace("public/user3", Sets.newHashSet("test"));
            admin.namespaces().grantPermissionOnNamespace("public/user3", "user3",
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
        }

        protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
            PulsarService pulsar = spy(new PulsarService(conf));

            setupBrokerMocks(pulsar);
            pulsar.start();

            return pulsar;
        }


        protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
            // Override default providers with mocked ones
            doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
            doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();

            Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
            doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

            doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
        }

        public static MockZooKeeper createMockZooKeeper() throws Exception {
            MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
            List<ACL> dummyAclList = new ArrayList<>(0);

            ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:5000",
                    "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

            zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
                      CreateMode.PERSISTENT);
            return zk;
        }

        public ZooKeeperClientFactory getZooKeeperClientFactory() {
            return mockZooKeeperClientFactory;
        }

        public OrderedExecutor getExecutor() {
            return sameThreadOrderedSafeExecutor;
        }

        protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {
                @Override
                public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                                                           int zkSessionTimeoutMillis) {
                    return CompletableFuture.completedFuture(mockZookKeeper);
                }
            };

        private BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {
                @Override
                public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                                         Optional<Class<? extends EnsemblePlacementPolicy>> placement,
                                         Map<String, Object> ignore) {
                    return mockBookKeeper;
                }

                @Override
                public void close() {
                }
            };

        protected void cleanup() throws Exception {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (pulsar != null) {
                    pulsar.close();
                }
                if (mockBookKeeper != null) {
                    mockBookKeeper.shutdown();
                }
                if (mockZookKeeper != null) {
                    mockZookKeeper.shutdown();
                }
                if (sameThreadOrderedSafeExecutor != null) {
                    sameThreadOrderedSafeExecutor.shutdown();
                }
                if (bkExecutor != null) {
                    bkExecutor.shutdown();
                }
            } catch (Exception e) {
                log.warn("Failed to clean up mocked pulsar service:", e);
                throw e;
            }
        }
    };

}
