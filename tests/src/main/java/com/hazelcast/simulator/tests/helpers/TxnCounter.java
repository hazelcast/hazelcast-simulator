package com.hazelcast.simulator.tests.helpers;

import java.io.Serializable;

public class TxnCounter implements Serializable {

    public long committed ;
    public long rolled;
    public long failedRoles;

    public TxnCounter() {
    }

    public void add(TxnCounter c) {
        committed += c.committed;
        rolled += c.rolled;
        failedRoles += c.failedRoles;
    }

    @Override
    public String toString() {
        return "TxnCounter{"
                + "committed=" + committed
                + ", rolled=" + rolled
                + ", failedRoles=" + failedRoles
                + '}';
    }
}
