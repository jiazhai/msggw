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
    name = "BKMocks",
    srcs = ["MockBKCluster.java", "MockBKClient.java"],
    deps = [
        "@managed_ledger_test//jar",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("org.apache.bookkeeper:bookkeeper-common"),
        artifact("org.apache.bookkeeper:bookkeeper-server"),
        artifact("org.apache.pulsar:managed-ledger-original"),
        artifact("org.apache.zookeeper:zookeeper"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "MLTableTest",
    srcs = ["MLTableTest.java"],
    deps = [
        "//mltable/src/main/java:mltable",
        ":BKMocks",
        artifact("com.google.guava:guava"),
        artifact("junit:junit"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
        artifact("org.slf4j:slf4j-simple"),
    ],
)

