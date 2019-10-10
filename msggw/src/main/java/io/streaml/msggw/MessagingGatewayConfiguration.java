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

import com.google.common.base.Strings;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Properties;

import lombok.Setter;
import lombok.Getter;

import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.util.FieldParser;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.proxy.server.ProxyConfiguration;

@Getter
@Setter
public class MessagingGatewayConfiguration extends ProxyConfiguration implements PulsarConfiguration {
    @Category
    private static final String CATEGORY_MSGGW = "Messaging gateway";

    @Category
    private static final String CATEGORY_KAFKA = "Kafka";

    @Category
    private static final String CATEGORY_HTTP = "Http";

    @Category
    private static final String CATEGORY_STORAGE_ML = "Storage (Managed Ledger)";

    @FieldContext(category = CATEGORY_MSGGW,
                  doc = "Address (hostname or IP) to advertize as, must be routable by connecting clients")
    private String advertizedAddress = null;

    @FieldContext(category = CATEGORY_MSGGW,
                  doc = "Enable the kafka protocol handler")
    private boolean kafkaEnabled = true;

    @FieldContext(category = CATEGORY_KAFKA,
                  doc = "The port that the kafka protocol handler should listen on")
    private int kafkaPort = 5567;

    @FieldContext(category = CATEGORY_KAFKA,
                  doc = "The name of the kafka cluster presented by the kafka protocol handler")
    private String kafkaPulsarClusterName = "pulsar-kafka";

    @FieldContext(category = CATEGORY_KAFKA,
            doc = "The default Pulsar namespace kafka topics will be placed in")
    private String kafkaPulsarDefaultNamespace = "public/default";

    @FieldContext(category = CATEGORY_MSGGW,
                  doc = "Enable the Http endpoint")
    private boolean httpEnabled = true;

    @FieldContext(category = CATEGORY_HTTP,
                  doc = " Enable data api resources, for producing and consuming via REST")
    private boolean httpDataApiEnabled = true;

    @FieldContext(category = CATEGORY_HTTP,
                  doc = "Enable admin api resources")
    private boolean adminApiEnabled = true;

    @FieldContext(category = CATEGORY_HTTP,
                  doc = "Allow the gateway to work as a reverse proxy")
    private boolean reverseProxyEnabled = true;

    @FieldContext(category = CATEGORY_HTTP,
                  doc = "Enable metrics on the gateway")
    private boolean metricsEnabled = true;

    @FieldContext(category = CATEGORY_HTTP,
                  doc = "Enable vip status reporting")
    private boolean vipStatusEnabled = true;

    @FieldContext(category = CATEGORY_HTTP,
                  doc = "Enable the pulsar websocket proxy")
    private boolean webSocketProxyEnabled = true;

    @FieldContext(category = CATEGORY_MSGGW,
                  doc = "Enable the pulsar binary protocol proxy")
    private boolean binaryProtocolProxyEnabled = true;

    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Number of bookies to use when creating a ledger"
    )
    private int managedLedgerDefaultEnsembleSize = 1;
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Number of copies to store for each message"
    )
    private int managedLedgerDefaultWriteQuorum = 1;
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Number of guaranteed copies (acks to wait before write is complete)"
    )
    private int managedLedgerDefaultAckQuorum = 1;

    static MessagingGatewayConfiguration loadFromFile(String configFile) throws IOException {
        Properties properties = new Properties();

        try (FileInputStream is = new FileInputStream(configFile)) {
            properties.load(is);
        }

        MessagingGatewayConfiguration config = new MessagingGatewayConfiguration();
        config.setProperties(properties);
        updateFields(properties, config, config.getClass());
        return config;
    }

    static MessagingGatewayConfiguration loadFromClasspath() throws IOException {
        Properties properties = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream("msggw.conf");
        if (is == null) {
            throw new FileNotFoundException("msggw.conf not found in classpath");
        }
        try {
            properties.load(is);
        } finally {
            is.close();
        }

        MessagingGatewayConfiguration config = new MessagingGatewayConfiguration();
        config.setProperties(properties);
        updateFields(properties, config, config.getClass());
        return config;
    }

    private static void updateFields(Properties properties, Object obj, Class<?> clazz)
            throws IllegalArgumentException {
        if (PulsarConfiguration.class.isAssignableFrom(clazz)) {
            Field[] fields = clazz.getDeclaredFields();
            Arrays.stream(fields).forEach(f -> {
                    if (properties.containsKey(f.getName())) {
                        try {
                            f.setAccessible(true);
                            String v = properties.getProperty(f.getName());
                            if (!Strings.isNullOrEmpty(v)) {
                                f.set(obj, FieldParser.value(v, f));
                            }
                        } catch (Exception e) {
                            throw new IllegalArgumentException(
                                    String.format("failed to initialize %s field while setting value %s",
                                                  f.getName(), properties.get(f.getName())), e);
                        }
                    }
                });
            updateFields(properties, obj, clazz.getSuperclass());
        }
    }

    public static MessagingGatewayConfiguration fromServiceConfiguration(ServiceConfiguration serviceConfiguration) {
        final MessagingGatewayConfiguration convertedConf = new MessagingGatewayConfiguration();

        Class<?> clazz = ServiceConfiguration.class;
        while (PulsarConfiguration.class.isAssignableFrom(clazz)) {
            Field[] confFields = clazz.getDeclaredFields();
            Arrays.stream(confFields).forEach(confField -> {
                try {
                    Field convertedConfField;
                    try {
                        convertedConfField = MessagingGatewayConfiguration.class.getDeclaredField(confField.getName());
                    } catch (NoSuchFieldException e) {
                        convertedConfField = MessagingGatewayConfiguration.class.getSuperclass().getDeclaredField(confField.getName());
                    }
                    confField.setAccessible(true);
                    if (!Modifier.isStatic(convertedConfField.getModifiers())) {
                        convertedConfField.setAccessible(true);
                        convertedConfField.set(convertedConf, confField.get(serviceConfiguration));
                    }
                } catch (NoSuchFieldException e) {
                    // ignore
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Exception caused while converting configuration", e);
                }
            });
            clazz = clazz.getSuperclass();
        }
        return convertedConf;
    }

    public ServiceConfiguration toServiceConfiguration()
            throws RuntimeException {
        final ServiceConfiguration convertedConf = new ServiceConfiguration();

        Class<?> clazz = this.getClass();
        while (PulsarConfiguration.class.isAssignableFrom(clazz)) {
            Field[] confFields = clazz.getDeclaredFields();
            Arrays.stream(confFields).forEach(confField -> {
                    try {
                        Field convertedConfField = ServiceConfiguration.class.getDeclaredField(confField.getName());
                        confField.setAccessible(true);
                        if (!Modifier.isStatic(convertedConfField.getModifiers())) {
                            convertedConfField.setAccessible(true);
                            convertedConfField.set(convertedConf, confField.get(this));
                        }
                    } catch (NoSuchFieldException e) {
                        // ignore
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Exception caused while converting configuration", e);
                    }
                });
            clazz = clazz.getSuperclass();
        }
        return convertedConf;
    }
}
