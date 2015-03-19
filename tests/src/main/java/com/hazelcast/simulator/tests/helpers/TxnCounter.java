package com.hazelcast.simulator.tests.helpers;

import java.io.Serializable;

public class TxnCounter implements Serializable {

    public long committed = 0;
    public long rolled = 0;
    public long failedRoles = 0;

    public TxnCounter() {
    }

    public void add(TxnCounter c) {
        committed += c.committed;
        rolled += c.rolled;
        failedRoles += c.failedRoles;
    }

    @Override
    public String toString() {
        return "TxnCounter{" +
                "committed=" + committed +
                ", rolled=" + rolled +
                ", failedRoles=" + failedRoles +
                '}';
    }
}