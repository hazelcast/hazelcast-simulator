package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.ProbesResultXmlElements;
import com.hazelcast.simulator.probes.probes.Result;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.text.NumberFormat;
import java.util.Locale;

import static java.lang.String.format;

public class OperationsPerSecResult implements Result<OperationsPerSecResult> {

    public static final String XML_TYPE = OperationsPerSecResult.class.getSimpleName();

    private final long invocations;
    private final double operationsPerSecond;

    public OperationsPerSecResult(long invocations, double operationsPerSecond) {
        this.invocations = invocations;
        this.operationsPerSecond = operationsPerSecond;
    }

    @Override
    public OperationsPerSecResult combine(OperationsPerSecResult other) {
        if (other == null) {
            return this;
        }
        return new OperationsPerSecResult(
                invocations + other.invocations,
                operationsPerSecond + other.operationsPerSecond);
    }

    @Override
    public String toHumanString() {
        NumberFormat formatter = NumberFormat.getInstance(Locale.US);
        return format("%15s ops %15s op/s", formatter.format(invocations), formatter.format(operationsPerSecond));
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        try {
            writer.writeStartElement(ProbesResultXmlElements.INVOCATIONS.string);
            writer.writeCharacters(Long.toString(invocations));
            writer.writeEndElement();
            writer.writeStartElement(ProbesResultXmlElements.OPERATIONS_PER_SECOND.string);
            writer.writeCharacters(Double.toString(operationsPerSecond));
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

        OperationsPerSecResult that = (OperationsPerSecResult) o;

        if (that.invocations != invocations) {
            return false;
        }
        if (Double.compare(that.operationsPerSecond, operationsPerSecond) != 0) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        long result = Double.doubleToLongBits(operationsPerSecond);
        result = 31 * result + invocations;
        return (int) (result ^ (result >>> 32));
    }
}
