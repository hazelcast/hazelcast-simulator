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
            writer.writeStartElement(ProbesResultXmlElements.INVOCATIONS.getName());
            writer.writeCharacters(Long.toString(invocations));
            writer.writeEndElement();
            writer.writeStartElement(ProbesResultXmlElements.OPERATIONS_PER_SECOND.getName());
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
