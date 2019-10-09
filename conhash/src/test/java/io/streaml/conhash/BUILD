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

load("@rules_jvm_external//:defs.bzl", "artifact")

java_test(
    name = "CHashGroupZKTest",
    srcs = ["CHashGroupZKTest.java"],
    size = "small",
    jvm_flags = ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"],
    deps = [
        "//conhash/src/main/java/io/streaml/conhash:lib",
        artifact("com.google.guava:guava"),
        artifact("org.hamcrest:java-hamcrest"),
        artifact("junit:junit"),
        artifact("org.slf4j:slf4j-api"),
        artifact("org.slf4j:slf4j-simple"),
        artifact("org.apache.zookeeper:zookeeper"),
        artifact("org.apache.commons:commons-lang3"),
        "@managed_ledger_test//jar",
    ],
)
