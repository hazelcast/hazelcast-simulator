package com.hazelcast.stabilizer.common;

public class AddressPair {
    public String publicAddress;
    public String privateAddress;

    @Override
    public String toString() {
        return "AddressPair{" +
                "publicAddress='" + publicAddress + '\'' +
                ", privateAddress='" + privateAddress + '\'' +
                '}';
    }
}
