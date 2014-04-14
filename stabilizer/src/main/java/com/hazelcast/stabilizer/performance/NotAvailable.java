package com.hazelcast.stabilizer.performance;

public class NotAvailable implements Performance<NotAvailable> {

    @Override
    public NotAvailable merge(NotAvailable performance) {
        return this;
    }

    @Override
    public String toHumanString() {
        return "No Performance data available";
    }
}

