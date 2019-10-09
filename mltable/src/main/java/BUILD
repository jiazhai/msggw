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
    name = "mltable",
    srcs = glob(["**/*.java"]),
    deps = [
        "//mltable/src/main/resources:JavaMLTableProtobufs",
        artifact("org.slf4j:slf4j-api"),
        artifact("org.apache.bookkeeper:bookkeeper-server"),
        artifact("org.apache.pulsar:managed-ledger-original"),
        artifact("com.google.guava:guava"),
        artifact("com.google.protobuf:protobuf-java"),
    ],
)
