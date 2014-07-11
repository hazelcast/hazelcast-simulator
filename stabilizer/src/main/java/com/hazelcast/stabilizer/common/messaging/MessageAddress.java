package com.hazelcast.stabilizer.common.messaging;

import java.io.Serializable;

public class MessageAddress implements Serializable {
    public static final String BROADCAST_PREFIX = "*";
    public static final String RANDOM_PREFIX = "R";
    public static final String OLDEST_MEMBER_PREFIX = "O";

    private String agentAddress;
    private String workerAddress;
    private String testAddress;

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

    public static class MessageAddressBuilder {
        private String agentAddress;
        private String workerAddress;
        private String testAddress;

        public MessageAddressBuilder toAllAgents() {
            agentAddress = BROADCAST_PREFIX;
            return this;
        }

        public MessageAddressBuilder toRandomAgent() {
            agentAddress = RANDOM_PREFIX;
            return this;
        }

        public MessageAddressBuilder toAllWorkers() {
            workerAddress = BROADCAST_PREFIX;
            return this;
        }

        public MessageAddressBuilder toRandomWorker() {
            workerAddress = RANDOM_PREFIX;
            return this;
        }

        public MessageAddressBuilder toOldestMember() {
            agentAddress = BROADCAST_PREFIX;
            workerAddress = OLDEST_MEMBER_PREFIX;
            return this;
        }

        public MessageAddressBuilder toAllTests() {
            testAddress = BROADCAST_PREFIX;
            return this;
        }

        public MessageAddressBuilder toRandomTest() {
            testAddress = RANDOM_PREFIX;
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
