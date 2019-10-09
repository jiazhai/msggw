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
    name = "HttpTest",
    srcs = ["HttpTest.java"],
    deps = [
        "//msggw/src/main/java:lib",
        "//msggw/src/test/java/io/streaml/msggw:unit-common",
        artifact("com.github.ben-manes.caffeine:caffeine"),
        artifact("com.google.guava:guava"),
        artifact("javax.ws.rs:javax.ws.rs-api"),
        artifact("junit:junit"),
        artifact("org.apache.pulsar:pulsar-broker-common"),
        artifact("org.apache.pulsar:pulsar-client-admin-original"),        
        artifact("org.apache.pulsar:pulsar-client-api"),
        artifact("org.apache.pulsar:pulsar-zookeeper-utils"),
        artifact("org.glassfish.jersey.core:jersey-client"),
        artifact("org.glassfish.jersey.core:jersey-common"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("org.slf4j:slf4j-api"),
    ],
)
