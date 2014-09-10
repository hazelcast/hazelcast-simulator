package com.hazelcast.stabilizer.common.probes;

import java.io.Serializable;

public interface Result<R extends Result> extends Serializable {
    R combine(R other);
    String toHumanString();
}
