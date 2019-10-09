#
# Copyright 2019 Streamlio, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
package(default_visibility = ["//visibility:public"])

load("@rules_jvm_external//:defs.bzl", "artifact")

java_library(
    name = "lib",
    srcs = glob(["**/*.java"]),
    deps = [
        "//conhash/src/main/java/io/streaml/conhash:lib",
        "//mltable/src/main/java:mltable",
        "//msggw:lombok",
        "//msggw/src/main/resources:JavaKafkaProtobufs",
        artifact("com.beust:jcommander"),
        artifact("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider"),
        artifact("com.github.ben-manes.caffeine:caffeine"),
        artifact("com.google.guava:guava"),
        artifact("com.google.protobuf:protobuf-java"),
        artifact("com.lmax:disruptor"),
        artifact("commons-io:commons-io"),
        artifact("io.netty:netty-all"),
        artifact("io.prometheus:simpleclient"),
        artifact("io.prometheus:simpleclient_hotspot"),
        artifact("io.prometheus:simpleclient_servlet"),
        artifact("javax.servlet:javax.servlet-api"),
        artifact("javax.ws.rs:javax.ws.rs-api"),
        artifact("org.apache.commons:commons-lang3"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.bookkeeper:bookkeeper-common"),
        artifact("org.apache.bookkeeper:bookkeeper-server"),
        artifact("org.apache.pulsar:managed-ledger-original"),
        artifact("org.apache.pulsar:pulsar-broker-common"),
        artifact("org.apache.pulsar:pulsar-client-admin-original"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.apache.pulsar:pulsar-client-original"),
        artifact("org.apache.pulsar:pulsar-common"),
        artifact("org.apache.pulsar:pulsar-proxy"),
        artifact("org.apache.pulsar:pulsar-websocket"),
        artifact("org.apache.pulsar:pulsar-zookeeper-utils"),
        artifact("org.apache.zookeeper:zookeeper"),
        artifact("org.eclipse.jetty:jetty-proxy"),
        artifact("org.eclipse.jetty:jetty-servlet"),
        artifact("org.glassfish.jersey.containers:jersey-container-servlet-core"),
        artifact("org.glassfish.jersey.core:jersey-server"),
        artifact("org.projectlombok:lombok"),
        artifact("org.slf4j:jul-to-slf4j"),
        artifact("org.slf4j:slf4j-api"),
    ],
)
