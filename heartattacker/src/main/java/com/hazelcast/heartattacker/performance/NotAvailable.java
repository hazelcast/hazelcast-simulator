package com.hazelcast.heartattacker.performance;

public class NotAvailable implements Performance{

    @Override
    public String toHumanString() {
        return "No Performance data available";
    }
}

