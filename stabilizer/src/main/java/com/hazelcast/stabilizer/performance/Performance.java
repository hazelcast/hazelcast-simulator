package com.hazelcast.stabilizer.performance;

import java.io.Serializable;

public interface Performance<P extends Performance> extends Serializable {

    String toHumanString();

    P merge(P performance);
}
