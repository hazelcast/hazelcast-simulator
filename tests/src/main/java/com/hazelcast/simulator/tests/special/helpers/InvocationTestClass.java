package com.hazelcast.simulator.tests.special.helpers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class InvocationTestClass {

    private volatile long invokeCounter;

    public static String getSource() {
        return "package com.hazelcast.simulator.tests.special.helpers;\n\n"
                + "public class InvocationTestClass {\n\n"
                + "    private volatile long invokeCounter;\n\n"
                + "    public void shouldBeCalled() {\n"
                + "        invokeCounter++;\n"
                + "    }\n\n"
                + "    public long getInvokeCounter() {\n"
                + "        return invokeCounter;\n"
                + "    }\n"
                + "}\n";
    }

    @SuppressFBWarnings({"VO_VOLATILE_INCREMENT"})
    public void shouldBeCalled() {
        invokeCounter++;
    }

    public long getInvokeCounter() {
        return invokeCounter;
    }
}
