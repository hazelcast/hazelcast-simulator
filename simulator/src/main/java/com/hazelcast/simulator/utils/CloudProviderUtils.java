package com.hazelcast.simulator.utils;

public final class CloudProviderUtils {

    public static final String AWS_EC2 = "aws-ec2";

    private CloudProviderUtils() {
    }

    public static boolean isStatic(String cloudProvider) {
        return "static".equals(cloudProvider);
    }

    public static boolean isEC2(String cloudProvider) {
        return AWS_EC2.equals(cloudProvider);
    }
}
