package com.hazelcast.simulator.utils;

public final class CloudProviderUtils {

    public static final String PROVIDER_STATIC = "static";
    public static final String PROVIDER_EC2 = "aws-ec2";
    public static final String PROVIDER_GCE = "google-compute-engine";

    private CloudProviderUtils() {
    }

    public static boolean isStatic(String cloudProvider) {
        return PROVIDER_STATIC.equals(cloudProvider);
    }

    public static boolean isEC2(String cloudProvider) {
        return PROVIDER_EC2.equals(cloudProvider);
    }

    public static boolean isGCE(String cloudProvider) {
        return PROVIDER_GCE.equals(cloudProvider);
    }
}
