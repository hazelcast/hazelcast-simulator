package com.hazelcast.simulator.utils;

public final class CloudProviderUtils {

    private CloudProviderUtils() {
    }

    public static boolean isStatic(String cloudProvider) {
        return "static".equals(cloudProvider);
    }

    public static boolean isEC2(String cloudProvider) {
        return "aws-ec2".equals(cloudProvider);
    }
}
