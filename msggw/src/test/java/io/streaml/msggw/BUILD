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
    name = "unit-common",
    srcs = glob(["MockPulsarCluster.java"]),
    resources = ["//msggw/src/test/resources:log-config"],
    deps = [
        "@bookkeeper_server_test//jar",
        "@managed_ledger_test//jar",
        "@pulsar_broker_test//jar",
        artifact("com.google.guava:guava"),
        artifact("io.jsonwebtoken:jjwt-api"),
        artifact("org.apache.bookkeeper:bookkeeper-common"),
        artifact("org.apache.bookkeeper:bookkeeper-server"),
        artifact("org.apache.pulsar:pulsar-broker"),
        artifact("org.apache.pulsar:pulsar-broker-common"),
        artifact("org.apache.pulsar:pulsar-client-admin-original"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.apache.pulsar:pulsar-common"),
        artifact("org.apache.pulsar:pulsar-zookeeper-utils"),
        artifact("org.apache.zookeeper:zookeeper"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "ProducerThreadTest",
    srcs = ["ProducerThreadTest.java"],
    size = "small",
    timeout = "long",
    deps = [
        "//msggw/src/main/java:lib",
        ":unit-common",
        artifact("com.github.ben-manes.caffeine:caffeine"),
        artifact("junit:junit"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "AlertableBatchEventProcessorTest",
    srcs = ["AlertableBatchEventProcessorTest.java"],
    size = "small",
    deps = [
        "//msggw/src/main/java:lib",
        ":unit-common",
        artifact("com.lmax:disruptor"),
        artifact("junit:junit"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
    ],
)
