package com.hazelcast.heartattacker.performance;

import java.io.Serializable;

public interface Performance extends Serializable {

    String toHumanString();
}
