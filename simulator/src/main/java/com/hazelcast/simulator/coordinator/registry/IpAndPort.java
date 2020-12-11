package com.hazelcast.simulator.coordinator.registry;

import static java.util.Objects.requireNonNull;

public final class IpAndPort {
    private final String ip;
    private final int port;

    public IpAndPort(String ip, int port) {
        requireNonNull(ip, "IP address cannot be null");
        this.ip = ip;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IpAndPort ipAndPort = (IpAndPort) o;

        if (port != ipAndPort.port) return false;
        return ip.equals(ipAndPort.ip);
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return ip + ':' + port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
