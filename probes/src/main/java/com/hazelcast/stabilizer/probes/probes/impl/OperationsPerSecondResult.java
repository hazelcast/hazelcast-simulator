package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.Result;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.text.NumberFormat;
import java.util.Locale;

public class OperationsPerSecondResult implements Result<OperationsPerSecondResult> {
    private final double operationsPerSecond;
    public OperationsPerSecondResult(double operationsPerSecond) {
        this.operationsPerSecond = operationsPerSecond;
    }

    @Override
    public OperationsPerSecondResult combine(OperationsPerSecondResult other) {
        if (other == null) {
            return this;
        }
        return new OperationsPerSecondResult(operationsPerSecond + other.operationsPerSecond);
    }

    @Override
    public String toHumanString() {
        NumberFormat floatFormat = NumberFormat.getInstance(Locale.US);
        return "Operations / second: " + floatFormat.format(operationsPerSecond);
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        try {
            writer.writeStartElement("operations-per-second");
            writer.writeCharacters(Double.toString(operationsPerSecond));
            writer.writeEndElement();
        } catch (XMLStreamException e) {
            throw new IllegalStateException("Error while writing probe output", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationsPerSecondResult that = (OperationsPerSecondResult) o;

        if (Double.compare(that.operationsPerSecond, operationsPerSecond) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(operationsPerSecond);
        return (int) (temp ^ (temp >>> 32));
    }
}
