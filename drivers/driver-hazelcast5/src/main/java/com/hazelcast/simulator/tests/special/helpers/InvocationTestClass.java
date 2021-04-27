/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    @SuppressFBWarnings({"VO_VOLATILE_INCREMENT"})
    public void shouldBeCalled() {
        invokeCounter++;
    }

    public long getInvokeCounter() {
        return invokeCounter;
    }
}
