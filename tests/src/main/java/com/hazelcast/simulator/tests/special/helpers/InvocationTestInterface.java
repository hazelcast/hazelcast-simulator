package com.hazelcast.simulator.tests.special.helpers;

public interface InvocationTestInterface {

    void shouldBeCalled();

    long getInvokeCounter();
}
