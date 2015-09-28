/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.probes.probes;

public enum ProbesResultXmlElements {

    PROBES_RESULT("probes-result"),
    PROBE("probe"),
    PROBE_NAME("name"),

    LATENCY("latency"),
    INVOCATIONS("invocations"),
    THROUGHPUT("throughput");

    private final String string;

    ProbesResultXmlElements(String string) {
        this.string = string;
    }

    public boolean matches(String elementName) {
        return string.equals(elementName);
    }

    public String getName() {
        return string;
    }
}
