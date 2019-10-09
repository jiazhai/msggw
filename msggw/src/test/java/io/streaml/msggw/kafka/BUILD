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

java_test(
    name = "CodecTest",
    srcs = ["CodecTest.java"],
    size = "small",
    deps = [
        "//msggw/src/main/java:lib",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("junit:junit"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "MessageIdUtilsTest",
    srcs = ["MessageIdUtilsTest.java"],
    size = "small",
    deps = [
        "//msggw/src/main/java:lib",
        artifact("junit:junit"),
        artifact("org.apache.pulsar:pulsar-client-original"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "FetcherTest",
    srcs = ["FetcherTest.java", "MockTimer.java"],
    size = "small",
    timeout = "long",
    deps = [
        "//msggw/src/main/java:lib",
        "//msggw/src/test/java/io/streaml/msggw:unit-common",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("junit:junit"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_library(
    name = "Mocks",
    srcs = ["MockConsumerGroupStorage.java", "MockCHashGroup.java"],
    deps = [
        "//conhash/src/main/java/io/streaml/conhash:lib",
        "//msggw/src/main/java:lib",
        "//msggw/src/main/resources:JavaKafkaProtobufs",
        artifact("com.google.guava:guava"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_library(
    name = "HandlerTestBase",
    srcs = ["HandlerTestBase.java"],
    deps = [
        ":Mocks",
        "//conhash/src/main/java/io/streaml/conhash:lib",
        "//msggw/src/main/java:lib",
        "//msggw/src/test/java/io/streaml/msggw:unit-common",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("junit:junit"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "HandlerTest",
    srcs = ["HandlerTest.java"],
    jvm_flags = ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"],
    size = "medium",
    timeout = "long",
    deps = [
        ":HandlerTestBase",
        artifact("com.google.guava:guava"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-simple"),
    ],
)

java_test(
    name = "HandlerAuthTest",
    srcs = ["HandlerAuthTest.java"],
    jvm_flags = ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"],
    size = "medium",
    timeout = "long",
    deps = [
        ":HandlerTestBase",
        artifact("com.google.guava:guava"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-simple"),
    ],
)

java_test(
    name = "HandlerOffsetCommitMultipleTopicsTest",
    srcs = ["HandlerOffsetCommitMultipleTopicsTest.java"],
    size = "medium",
    timeout = "long",
    tags = ["exclusive", "manual"],
    deps = [
        ":HandlerTestBase",
        artifact("org.hamcrest:java-hamcrest"),
    ],
)

java_test(
    name = "HandlerMultiClientConsumerGroupTest",
    srcs = ["HandlerMultiClientConsumerGroupTest.java"],
    size = "medium",
    timeout = "long",
    tags = ["exclusive"],
    deps = [
        ":HandlerTestBase",
        "//msggw/src/test/java/io/streaml/msggw:unit-common",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "HandlerConsumerGroupHandoverTest",
    srcs = ["HandlerConsumerGroupHandoverTest.java"],
    size = "medium",
    timeout = "long",
    tags = ["exclusive"],
    deps = [
        ":HandlerTestBase",
        "//msggw/src/main/java:lib",
        artifact("com.google.guava:guava"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "HandlerOffsetCommitSingleTopicTest",
    srcs = ["HandlerOffsetCommitSingleTopicTest.java"],
    size = "medium",
    timeout = "long",
    tags = ["exclusive"],
    deps = [
        ":HandlerTestBase",
        artifact("com.google.guava:guava"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
    ],
)

java_test(
    name = "ConsumerGroupsTest",
    srcs = ["ConsumerGroupsTest.java"],
    size = "small",
    deps = [
        artifact("org.slf4j:slf4j-simple"),
        ":Mocks",
        "//msggw/src/main/java:lib",
        artifact("com.google.guava:guava"),
        artifact("com.google.guava:guava-testlib"),
        artifact("com.google.protobuf:protobuf-java"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "KafkaServiceTest",
    srcs = ["KafkaServiceTest.java"],
    size = "small",
    deps = [
        ":HandlerTestBase",
        ":Mocks",
        "//msggw/src/main/java:lib",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "NodeIdsTest",
    srcs = ["NodeIdsTest.java"],
    size = "small",
    jvm_flags = ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"],
    deps = [
        artifact("org.slf4j:slf4j-simple"),
        "//msggw/src/main/java:lib",
        artifact("com.google.guava:guava"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("junit:junit"),
        artifact("org.slf4j:slf4j-api"),
        artifact("org.apache.zookeeper:zookeeper"),
        artifact("org.apache.commons:commons-lang3"),
        "@managed_ledger_test//jar",
    ],
)

java_test(
    name = "MultiHandlerTest",
    srcs = ["MultiHandlerTest.java", "MockConsumerGroupStorage.java"],
    size = "medium",
    timeout = "long",
    tags = ["exclusive"],
    deps = [
        "//msggw/src/main/java:lib",
        "//msggw/src/test/java/io/streaml/msggw:unit-common",
        "//msggw/src/main/resources:JavaKafkaProtobufs",
        "//conhash/src/main/java/io/streaml/conhash:lib",
        artifact("com.google.guava:guava"),
        artifact("io.netty:netty-all"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("junit:junit"),
        artifact("org.mockito:mockito-core"),
        artifact("org.slf4j:slf4j-api"),
    ],
)

java_test(
    name = "MLTableConsumerGroupStorageTest",
    srcs = ["MLTableConsumerGroupStorageTest.java"],
    jvm_flags = ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"],
    size = "medium",
    deps = [
        artifact("org.slf4j:slf4j-simple"),
        "//msggw/src/main/java:lib",
        "//mltable/src/test/java/io/streaml/mltable:BKMocks",
        "//msggw/src/main/resources:JavaKafkaProtobufs",
        artifact("com.google.guava:guava"),
        artifact("org.apache.kafka:kafka-clients"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("junit:junit"),
        artifact("org.slf4j:slf4j-api"),
    ],
)
