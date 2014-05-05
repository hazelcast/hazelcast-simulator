package com.hazelcast.stabilizer.common;

public class AgentAddress {
    public String publicAddress;
    public String privateAddress;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgentAddress address = (AgentAddress) o;

        if (privateAddress != null ? !privateAddress.equals(address.privateAddress) : address.privateAddress != null)
            return false;
        if (publicAddress != null ? !publicAddress.equals(address.publicAddress) : address.publicAddress != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = publicAddress != null ? publicAddress.hashCode() : 0;
        result = 31 * result + (privateAddress != null ? privateAddress.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AddressPair{" +
                "publicAddress='" + publicAddress + '\'' +
                ", privateAddress='" + privateAddress + '\'' +
                '}';
    }
}
