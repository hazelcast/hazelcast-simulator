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
package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.ProbesResultXmlElements;
import com.hazelcast.simulator.probes.probes.Result;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class MaxLatencyResult implements Result<MaxLatencyResult> {

    public static final String XML_TYPE = MaxLatencyResult.class.getSimpleName();

    private final long maxLatencyMs;

    public MaxLatencyResult(long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    @Override
    public MaxLatencyResult combine(MaxLatencyResult other) {
        if (other == null) {
            return this;
        }
        return new MaxLatencyResult(Math.max(maxLatencyMs, other.maxLatencyMs));
    }

    @Override
    public String toHumanString() {
        return String.format("%s ms (maximum latency)", maxLatencyMs);
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        try {
            writer.writeStartElement(ProbesResultXmlElements.MAX_LATENCY.getName());
            writer.writeCharacters(Long.toString(maxLatencyMs));
            writer.writeEndElement();
        } catch (XMLStreamException e) {
            throw new IllegalStateException("Error while writing probe output", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MaxLatencyResult that = (MaxLatencyResult) o;
        if (maxLatencyMs != that.maxLatencyMs) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (maxLatencyMs ^ (maxLatencyMs >>> 32));
    }
}
