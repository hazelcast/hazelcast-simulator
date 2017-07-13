/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.simulator.boot;

import com.hazelcast.config.Config;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;

public class Options {
    final SimulatorProperties simulatorProperties = loadSimulatorProperties();
    final Set<String> ignoredClasspath = new HashSet<String>();
    int memberCount;
    int clientCount;
    long durationSeconds;
    String memberArgs = "";
    String clientArgs = "";
    Config memberConfig;
    TestCase testCase = new TestCase("");
    String sessionId;
    String licenseKey;

    public Options() {
        addIgnored(".m2/repository/aopalliance/aopalliance/",
                ".m2/repository/com/amazonaws/aws-java-sdk",
                ".m2/repository/com/fasterxml/jackson/",
                ".m2/repository/com/google/code/findbugs/",
                ".m2/repository/com/google/code/gson/gson/",
                ".m2/repository/com/google/guava/guava/",
                ".m2/repository/com/google/inject/",
                ".m2/repository/com/hazelcast/simulator/simulator/",
                ".m2/repository/com/hazelcast/simulator/simulator-boot/",
                ".m2/repository/com/hazelcast/simulator/tests-common/",
                ".m2/repository/com/jcraft/jsch.agentproxy",
                ".m2/repository/commons-codec/commons-codec/",
                ".m2/repository/commons-logging/commons-logging/",
                ".m2/repository/io/netty/netty-all/",
                ".m2/repository/javassist/javassist/",
                ".m2/repository/javax/annotation/",
                ".m2/repository/javax/cache/cache-api/",
                ".m2/repository/javax/inject/javax.inject/",
                ".m2/repository/javax/ws/rs/jsr311-api/",
                ".m2/repository/joda-time/joda-time/",
                ".m2/repository/junit/junit/",
                ".m2/repository/log4j/log4j/",
                ".m2/repository/net/java/dev/jna/jna",
                ".m2/repository/net/jcip/jcip-annotations/",
                ".m2/repository/net/schmizz/sshj/",
                ".m2/repository/net/sf/jopt-simple/jopt-simple/",
                ".m2/repository/org/99soft/guice/rocoto/",
                ".m2/repository/org/apache/commons/commons-lang3/",
                ".m2/repository/org/apache/httpcomponents/",
                ".m2/repository/org/apache/jclouds/",
                ".m2/repository/org/bouncycastle/",
                ".m2/repository/org/freemarker/freemarker/",
                ".m2/repository/org/hamcrest/hamcrest-core/",
                ".m2/repository/org/hdrhistogram/HdrHistogram/",
                ".m2/repository/org/slf4j/slf4j-api/",
                ".m2/repository/org/slf4j/slf4j-log4j12/",
                "/jre/lib/",
                "jdk/Contents/Home/lib",
                "/lib/idea_rt.jar");

        simulatorProperties.set("VERSION_SPEC", "bringmyown");
    }

    private void addIgnored(String... ignored) {
        Collections.addAll(this.ignoredClasspath, ignored);
    }
}
