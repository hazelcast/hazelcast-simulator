package com.hazelcast.stabilizer.common.messaging;

import java.io.Serializable;

public class MessageAddress implements Serializable {
    public static final String BROADCAST_PREFIX = "*";
    public static final String RANDOM_PREFIX = "R";

    private String agentAddress;
    private String workerAddress;

    public MessageAddress(String agentAddress, String workerAddress) {
        this.agentAddress = agentAddress;
        this.workerAddress = workerAddress;
    }

    public String getAgentAddress() {
        return agentAddress;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public static MessageAddressBuilder builder() {
        return new MessageAddressBuilder();
    }

    public static class MessageAddressBuilder {
        private String agentAddress;
        private String workerAddress;

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

        public MessageAddress build() {
            return new MessageAddress(agentAddress, workerAddress);
        }
    }
}
