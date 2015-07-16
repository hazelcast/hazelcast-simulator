/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.test;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

@SuppressWarnings("checkstyle:visibilitymodifier")
public final class Failure implements Serializable {

    public enum Type {

        WORKER_EXCEPTION("Worker exception", "workerException"),
        WORKER_TIMEOUT("Worker timeout", "workerTimeout"),
        WORKER_OOM("Worker out of memory error", "workerOOM"),
        WORKER_EXIT("Worker exit", "workerExit");

        private String humanReadable;
        private String id;

        Type(String humanReadable, String id) {
            this.humanReadable = humanReadable;
            this.id = id;
        }

        @Override
        public String toString() {
            return humanReadable;
        }

        public static Set<Type> fromPropertyValue(String propertyValue) {
            if (propertyValue == null || propertyValue.isEmpty()) {
                return Collections.emptySet();
            }
            Set<Type> result = new HashSet<Type>();
            StringTokenizer tokenizer = new StringTokenizer(propertyValue, ",");
            while (tokenizer.hasMoreTokens()) {
                String id = tokenizer.nextToken().trim();
                Type failureType = getById(id);
                result.add(failureType);
            }
            return result;
        }

        public static String getIdsAsString() {
            Type[] types = Type.values();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < types.length; i++) {
                builder.append(types[i].id);
                if (i < types.length - 1) {
                    builder.append(", ");
                }
            }
            return builder.toString();
        }

        public String getId() {
            return id;
        }

        private static Type getById(String id) {
            for (Type type : Type.values()) {
                if (type.id.equals(id)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown failure ID: '" + id + "'.");
        }
    }

    private static final long serialVersionUID = 1;

    public final Date time = new Date();

    public String message;
    public Type type;
    public String agentAddress;
    public String workerAddress;
    public String workerId;
    public String testId;
    public TestSuite testSuite;
    public String cause;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure[\n");
        sb.append("   message='").append(message).append("'\n");
        sb.append("   type='").append(type).append("'\n");
        sb.append("   agentAddress=").append(agentAddress).append("\n");
        sb.append("   time=").append(time).append("\n");
        sb.append("   workerAddress=").append(workerAddress).append("\n");
        sb.append("   workerId=").append(workerId).append("\n");

        TestCase testCase = testSuite.getTestCase(testId);
        if (testCase != null) {
            String prefix = "   test=";
            for (String aTestString : testCase.toString().split("\n")) {
                sb.append(prefix).append(aTestString).append("\n");
                prefix = "    ";
            }
        } else {
            sb.append("   test=").append(testId).append(" unknown").append("\n");
        }

        sb.append("   cause=").append(cause != null ? cause : "null").append("\n");
        sb.append("]");

        return sb.toString();
    }
}
