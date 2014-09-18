package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.Result;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.XMLEvent;

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

    public static MaxLatencyResult readFrom(XMLStreamReader reader) throws XMLStreamException {
        MaxLatencyResult result = null;
        Elements currentElement = null;
        while (reader.hasNext()) {
            int eventType = reader.next();
            switch (eventType) {
                case XMLEvent.CHARACTERS:
                    if (currentElement == Elements.MAX_LATENCY) {
                        String text = reader.getText();
                        result = new MaxLatencyResult(Long.parseLong(text));
                    }
                    break;
                case XMLEvent.END_ELEMENT:
                    String elementName = reader.getLocalName();
                    if ("probe".equals(elementName)) {
                        return result;
                    } else if ("max-latency".equals(elementName)) {
                            currentElement = null;
                    }
                case XMLEvent.START_ELEMENT:
                    elementName = reader.getLocalName();
                    if ("max-latency".equals(elementName)) {
                        currentElement = Elements.MAX_LATENCY;
                    }
                    break;
            }
        }
        return null;
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

    private enum Elements {
        MAX_LATENCY
    }
}
