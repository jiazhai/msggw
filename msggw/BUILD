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
    name = "lombok",
    exports = [
        artifact("org.projectlombok:lombok"),
    ],
    exported_plugins = [
        ":lombok_plugin"
    ],
)

java_plugin(
    name = "lombok_plugin",
    processor_class = "lombok.launch.AnnotationProcessorHider$AnnotationProcessor",
    deps = [
        artifact("org.projectlombok:lombok"),
    ],
    generates_api = 1,
)

java_binary(
    name = "msggw",
    main_class = "io.streaml.msggw.MessagingGatewayStarter",
    runtime_deps = [ "//msggw/src/main/java:lib",
                     artifact("org.apache.logging.log4j:log4j-core"),
                     artifact("org.apache.logging.log4j:log4j-slf4j-impl"),
                     ],
)



