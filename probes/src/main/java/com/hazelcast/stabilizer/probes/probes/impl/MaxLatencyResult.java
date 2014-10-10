package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.Result;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class MaxLatencyResult implements Result<MaxLatencyResult> {
    private final long maxLatency;

    public MaxLatencyResult(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    @Override
    public MaxLatencyResult combine(MaxLatencyResult other) {
        if (other == null) {
            return this;
        }
        return new MaxLatencyResult(Math.max(maxLatency, other.maxLatency));
    }

    @Override
    public String toHumanString() {
        return "Maximum latency: "+maxLatency+" ms.";
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        try {
            writer.writeStartElement("max-latency");
            writer.writeCharacters(Long.toString(maxLatency));
            writer.writeEndElement();
        } catch (XMLStreamException e) {
            throw new IllegalStateException("Error while writing probe output", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MaxLatencyResult that = (MaxLatencyResult) o;

        if (maxLatency != that.maxLatency) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (maxLatency ^ (maxLatency >>> 32));
    }

}
