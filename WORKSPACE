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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_jar")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "bazel_skylib",
    remote = "https://github.com/bazelbuild/bazel-skylib.git",
    #tag = "0.7.0",  # change this to use a different release,
    commit = "6741f733227dc68137512161a5ce6fcf283e3f58",
)

## check we have the right bazel version before continuing
load("@bazel_skylib//lib:versions.bzl", "versions")

versions.check(minimum_bazel_version = "0.21.0")

maven_server(
    name = "apache_snapshot_server",
    url = "https://repository.apache.org/content/groups/snapshots/",
)

##############################################################################################
#
# Java Maven dependencies
#
##############################################################################################
RULES_JVM_EXTERNAL_TAG = "2.2"
RULES_JVM_EXTERNAL_SHA = "f1203ce04e232ab6fdd81897cf0ff76f2c04c0741424d192f28e65ae752ce2d6"
http_archive(
    name = "rules_jvm_external",
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    sha256 = RULES_JVM_EXTERNAL_SHA,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

# BK and PULSAR versions only for java dependencies used in msggw
# Ideally we should bazelize the BK and Pulsar build, but that's a huge undertaking
BOOKKEEPER_VERSION = "4.9.1"
PULSAR_VERSION = "2.4.0"

CAFFEINE_VERSION = "2.6.2"
COMMONS_IO_VERSION = "2.4"
COMMONS_LANG3_VERSION = "3.4"
DISRUPTOR_VERSION = "3.4.2"
GUAVA_VERSION = "26.0-jre"
JACKSON_VERSION = "2.8.4"
JACKSON_VERSION = "2.9.8"
JAVAX_SERVLET_VERSION = "3.1.0"
JCOMMANDER_VERSION = "1.48"
JERSEY_VERSION = "2.27"
JETTY_VERSION = "9.4.12.v20180830"
KAFKA_VERSION = "2.0.0"
LOG4J_VERSION = "2.10.0"
LOMBOK_VERSION = "1.18.4"
NETTY_VERSION = "4.1.31.Final"
PROMETHEUS_VERSION = "0.5.0"
PROTOBUF_VERSION = "3.8.0"
SLF4J_VERSION = "1.7.25"
TESTCONTAINERS_VERSION = "1.10.1"
WS_API_VERSION = "2.1.1"

maven_install(
    artifacts = [
         "org.eclipse.jetty.websocket:websocket-server:%s" % JETTY_VERSION,
        "com.bettercloud:vault-java-driver:4.0.0",
        "com.beust:jcommander:%s" % JCOMMANDER_VERSION,
        "com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:%s" % JACKSON_VERSION,
        "com.github.ben-manes.caffeine:caffeine:%s" % CAFFEINE_VERSION,
        "com.google.code.gson:gson:2.8.5",
        "com.google.guava:guava-testlib:%s" % GUAVA_VERSION,
        "com.google.guava:guava:%s" % GUAVA_VERSION,
        "com.google.protobuf:protobuf-java:%s" % PROTOBUF_VERSION,
        "com.lmax:disruptor:%s" % DISRUPTOR_VERSION,
        "commons-io:commons-io:%s" % COMMONS_IO_VERSION,
        "io.kubernetes:client-java-api:3.0.0",
        "io.netty:netty-all:%s" % NETTY_VERSION,
        "io.prometheus:simpleclient:%s" % PROMETHEUS_VERSION,
        "io.prometheus:simpleclient_hotspot:%s" % PROMETHEUS_VERSION,
        "io.prometheus:simpleclient_servlet:%s" % PROMETHEUS_VERSION,
        "io.jsonwebtoken:jjwt-api:0.10.5",
        "javax.servlet:javax.servlet-api:%s" % JAVAX_SERVLET_VERSION,
        "javax.websocket:javax.websocket-client-api:jar:1.0",
        "javax.ws.rs:javax.ws.rs-api:%s" % WS_API_VERSION,
        "junit:junit:4.12",
        "org.apache.bookkeeper:bookkeeper-common:%s" % BOOKKEEPER_VERSION,
        "org.apache.bookkeeper:bookkeeper-server:%s" % BOOKKEEPER_VERSION,
        "org.apache.commons:commons-lang3:%s" % COMMONS_LANG3_VERSION,
        "org.apache.kafka:kafka-clients:%s" % KAFKA_VERSION,
        "org.apache.logging.log4j:log4j-core:%s" % LOG4J_VERSION,
        "org.apache.logging.log4j:log4j-slf4j-impl:%s" % LOG4J_VERSION,
        "org.apache.pulsar:managed-ledger-original:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-broker-common:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-broker:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-client-admin-original:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-client-api:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-client-original:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-common:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-proxy:%s" % PULSAR_VERSION,
        "org.apache.pulsar:pulsar-zookeeper-utils:%s" % PULSAR_VERSION,
        "org.apache.zookeeper:zookeeper:3.4.13",
        "org.eclipse.jetty:jetty-proxy:%s" % JETTY_VERSION,
        "org.eclipse.jetty:jetty-servlet:%s" % JETTY_VERSION,
        "org.glassfish.jersey.containers:jersey-container-servlet-core:%s" % JERSEY_VERSION,
        "org.glassfish.jersey.core:jersey-client:%s" % JERSEY_VERSION,
        "org.glassfish.jersey.core:jersey-common:%s" % JERSEY_VERSION,
        "org.glassfish.jersey.core:jersey-server:%s" % JERSEY_VERSION,
        "org.hamcrest:java-hamcrest:2.0.0.0",
        "org.mockito:mockito-core:1.9.5",
        "org.projectlombok:lombok:%s" % LOMBOK_VERSION,
        "org.slf4j:jul-to-slf4j:%s" % SLF4J_VERSION,
        "org.slf4j:slf4j-api:%s" % SLF4J_VERSION,
        "org.slf4j:slf4j-simple:%s" % SLF4J_VERSION,
        "org.testcontainers:pulsar:%s" % TESTCONTAINERS_VERSION,
        "org.testcontainers:testcontainers:%s" % TESTCONTAINERS_VERSION,
        "org.yaml:snakeyaml:1.23",
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
    excluded_artifacts = [
        "io.grpc:grpc-core",
        "org.slf4j:slf4j-log4j12",
    ],
)

http_jar(
    name = "pulsar_broker_test",
    url = "https://repo1.maven.org/maven2/org/apache/pulsar/pulsar-broker/%s/pulsar-broker-%s-tests.jar" \
            % (PULSAR_VERSION, PULSAR_VERSION)
)

http_jar(
    name = "managed_ledger_test",
    url = "https://repo1.maven.org/maven2/org/apache/pulsar/managed-ledger-original/%s/managed-ledger-original-%s-tests.jar" \
            % (PULSAR_VERSION, PULSAR_VERSION)
)

http_jar(
    name = "bookkeeper_server_test",
    url = "https://repo1.maven.org/maven2/org/apache/bookkeeper/bookkeeper-server/%s/bookkeeper-server-%s-tests.jar" \
            % (BOOKKEEPER_VERSION, BOOKKEEPER_VERSION)

)

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-%s" % (PROTOBUF_VERSION),
    urls = ["https://github.com/google/protobuf/archive/v%s.zip" % (PROTOBUF_VERSION)],
)
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

