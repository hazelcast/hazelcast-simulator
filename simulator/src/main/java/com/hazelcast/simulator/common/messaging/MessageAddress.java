package com.hazelcast.simulator.common.messaging;

import java.io.Serializable;

public class MessageAddress implements Serializable {

    public static final String BROADCAST = "*";
    public static final String RANDOM = "R";

    public static final String ALL_WORKERS_WITH_MEMBER = "*m";
    public static final String RANDOM_WORKER_WITH_MEMBER = "Rm";
    public static final String WORKER_WITH_OLDEST_MEMBER = "O";

    private final String agentAddress;
    private final String workerAddress;
    private final String testAddress;

    public MessageAddress(String agentAddress, String workerAddress, String testAddress) {
        this.agentAddress = agentAddress;
        this.workerAddress = workerAddress;
        this.testAddress = testAddress;
    }

    public String getAgentAddress() {
        return agentAddress;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public String getTestAddress() {
        return testAddress;
    }

    public static MessageAddressBuilder builder() {
        return new MessageAddressBuilder();
    }

    @Override
    public String toString() {
        String message = "MessageAddress{";
        if (agentAddress != null) {
            message += "agentAddress='" + agentAddress + '\'';
        }
        if (workerAddress != null) {
            message += "workerAddress='" + workerAddress + '\'';
        }
        if (testAddress != null) {
            message += "testAddress='" + testAddress + '\'';
        }
        message += '}';
        return message;
    }

    public static class MessageAddressBuilder {
        private String agentAddress;
        private String workerAddress;
        private String testAddress;

        public MessageAddressBuilder toAllAgents() {
            agentAddress = BROADCAST;
            return this;
        }

        public MessageAddressBuilder toRandomAgent() {
            agentAddress = RANDOM;
            return this;
        }

        public MessageAddressBuilder toAllWorkers() {
            workerAddress = BROADCAST;
            return this;
        }

        public MessageAddressBuilder toRandomWorker() {
            workerAddress = RANDOM;
            return this;
        }

        public MessageAddressBuilder toOldestMember() {
            if (agentAddress == null) {
                agentAddress = BROADCAST;
            }
            workerAddress = WORKER_WITH_OLDEST_MEMBER;
            return this;
        }

        public MessageAddressBuilder toWorkersWithClusterMember() {
            if (agentAddress == null) {
                agentAddress = BROADCAST;
            }
            workerAddress = ALL_WORKERS_WITH_MEMBER;
            return this;
        }

        public MessageAddressBuilder toRandomWorkerWithMember() {
            if (agentAddress == null) {
                agentAddress = BROADCAST;
            }
            workerAddress = RANDOM_WORKER_WITH_MEMBER;
            return this;
        }

        public MessageAddressBuilder toAllTests() {
            testAddress = BROADCAST;
            return this;
        }

        public MessageAddressBuilder toRandomTest() {
            testAddress = RANDOM;
            return this;
        }

        public MessageAddress build() {
            validate();
            return new MessageAddress(agentAddress, workerAddress, testAddress);
        }

        private void validate() {
            if (agentAddress == null && workerAddress != null) {
                throw new IllegalStateException("Agent address cannot be empty when worker address is specified");
            }
            if (workerAddress == null && testAddress != null) {
                throw new IllegalStateException("Worker address cannot be empty when test address is specified");
            }
        }
    }
}
