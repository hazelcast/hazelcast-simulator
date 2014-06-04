/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests;

import com.hazelcast.stabilizer.TestCase;

import java.io.Serializable;
import java.util.Date;

public class Failure implements Serializable {

    private static final long serialVersionUID = 1;

    public String message;
    public String type;
    public String agentAddress;
    public String workerAddress;
    public String workerId;
    public Date time = new Date();
    public String testId;
    public TestSuite testSuite;
    public String cause;

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Failure[\n");
        sb.append("   message='").append(message).append("'\n");
        sb.append("   type='").append(type).append("'\n");
        sb.append("   agentAddress=").append(agentAddress).append("\n");
        sb.append("   time=").append(time).append("\n");
        sb.append("   workerAddress=").append(workerAddress).append("\n");
        sb.append("   workerId=").append(workerId).append("\n");

        TestCase testCase = testSuite.getTestCase(testId);

        if (testCase != null) {
            String[] testString = testCase.toString().split("\n");
            sb.append("   test=").append(testString[0]).append("\n");
            for (int k = 1; k < testString.length; k++) {
                sb.append("    ").append(testString[k]).append("\n");
            }
        } else {
            sb.append("   test=").append("unknown").append("\n");
        }

        if (cause != null) {
            sb.append("   cause=").append(cause).append("\n");
        } else {
            sb.append("   cause=").append("null").append("\n");
        }

        sb.append("]");
        return sb.toString();
    }
}
