package com.hazelcast.simulator.common;

public enum CloudProvider {
    STATIC("static"),
    AWS_EC2("aws-ec2"),
    GCE("google-compute-engine");

    private final String description;

    CloudProvider(String description) {
        this.description = description;
    }

    static CloudProvider getFromProperties(String cloudProvider) {
        for (CloudProvider candidate : CloudProvider.values()) {
            if (candidate.description.equals(cloudProvider)) {
                return candidate;
            }
        }
        return null;
    }

    public String desc() {
        return description;
    }
}
