package com.hazelcast.simulator.tests.special.helpers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public class InvocationTestClass {

    private volatile long invokeCounter;

    public static String getSource() {
        return "package com.hazelcast.simulator.tests.special.helpers;" + NEW_LINE + NEW_LINE
                + "public class InvocationTestClass {" + NEW_LINE + NEW_LINE
                + "    private volatile long invokeCounter;" + NEW_LINE + NEW_LINE
                + "    public void shouldBeCalled() {" + NEW_LINE
                + "        invokeCounter++;" + NEW_LINE
                + "    }" + NEW_LINE + NEW_LINE
                + "    public long getInvokeCounter() {" + NEW_LINE
                + "        return invokeCounter;" + NEW_LINE
                + "    }" + NEW_LINE
                + "}" + NEW_LINE;
    }

    @SuppressFBWarnings({"VO_VOLATILE_INCREMENT" })
    public void shouldBeCalled() {
        invokeCounter++;
    }

    public long getInvokeCounter() {
        return invokeCounter;
    }
}
