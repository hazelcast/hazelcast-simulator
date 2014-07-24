package com.hazelcast.stabilizer.tests.queue.helpers;

import java.io.Serializable;
import java.util.Random;

public class TxnCounter implements Serializable {

    public long committed=0;
    public long rolled=0;

    public TxnCounter() {
    }

    public void add(TxnCounter c){
        committed += c.committed;
        rolled += c.rolled;
    }

    @Override
    public String toString() {
        return "TxnCounter{" +
                "committed=" + committed +
                ", rolled=" + rolled +
                '}';
    }
}