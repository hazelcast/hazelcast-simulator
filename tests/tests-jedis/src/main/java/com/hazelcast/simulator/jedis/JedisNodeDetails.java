package com.hazelcast.simulator.jedis;

import redis.clients.jedis.HostAndPort;

import java.util.Set;

public class JedisNodeDetails {

    private Set<HostAndPort> hostAndPortSet;

    private String password;

    public JedisNodeDetails() {
    }

    public JedisNodeDetails(Set<HostAndPort> hostAndPortSet, String password) {
        this.hostAndPortSet = hostAndPortSet;
        this.password = password;
    }

    public Set<HostAndPort> getHostAndPortSet() {
        return hostAndPortSet;
    }

    public void setHostAndPortSet(Set<HostAndPort> hostAndPortSet) {
        this.hostAndPortSet = hostAndPortSet;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
