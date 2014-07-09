package com.hazelcast.stabilizer.common.messaging;

import java.util.concurrent.Future;

public interface Predicate {
    public Future<Boolean> apply();
}
