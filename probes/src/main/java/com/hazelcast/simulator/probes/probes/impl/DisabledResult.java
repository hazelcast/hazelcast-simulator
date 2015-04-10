package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Result;

import javax.xml.stream.XMLStreamWriter;

public final class DisabledResult implements Result<DisabledResult> {

    @Override
    public DisabledResult combine(DisabledResult other) {
        return this;
    }

    @Override
    public String toHumanString() {
        return "Probe Disabled";
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj != null && obj.getClass().equals(DisabledResult.class));
    }
}
