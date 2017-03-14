package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;

/**
 * A test that stops by itself.
 */
public class StoppingTest  {

    @TimeStep
    public void timestep() {
        throw new StopException();
    }
}
