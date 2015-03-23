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
        return "Maximum latency: " + maxLatencyMs + " ms.";
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        try {
            writer.writeStartElement(ProbesResultXmlElements.MAX_LATENCY.string);
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
