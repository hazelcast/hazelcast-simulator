package com.hazelcast.stabilizer.common;

public class AgentAddress {
    public final  String publicAddress;
    public final String privateAddress;

    public AgentAddress(String publicAddress, String privateAddress) {
        if(publicAddress == null){
            throw new NullPointerException("publicAddress can't be null");
        }
        if(privateAddress == null){
            throw new NullPointerException("privateAddress can't be null");
        }
        this.publicAddress = publicAddress;
        this.privateAddress = privateAddress;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgentAddress address = (AgentAddress) o;

        if (!privateAddress.equals(address.privateAddress)) return false;
        if (!publicAddress.equals(address.publicAddress)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = publicAddress.hashCode();
        result = 31 * result + privateAddress.hashCode();
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
