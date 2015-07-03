package com.hazelcast.simulator.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public final class HostAddressPicker {

    private HostAddressPicker() {
    }

    public static String pickHostAddress() {
        NetworkInterface networkInterface = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            String loopbackHost = null;
            while (interfaces.hasMoreElements()) {
                networkInterface = interfaces.nextElement();
                if (!networkInterface.isUp()) {
                    continue;
                }
                if (networkInterface.isPointToPoint()) {
                    continue;
                }
                if (!networkInterface.isLoopback()) {
                    String candidateAddress = getHostAddressOrNull(networkInterface);
                    if (candidateAddress != null) {
                        return candidateAddress;
                    }
                } else {
                    if (loopbackHost == null) {
                        loopbackHost = getHostAddressOrNull(networkInterface);
                    }
                }
            }
            if (loopbackHost != null) {
                return loopbackHost;
            } else {
                throw new IllegalStateException("Cannot find local host address");
            }
        } catch (SocketException e) {
            throw new RuntimeException("Error during pickHostAddress() at network interface: " + networkInterface, e);
        }
    }

    private static String getHostAddressOrNull(NetworkInterface networkInterface) {
        Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
            InetAddress address = addresses.nextElement();
            if (!(address instanceof Inet6Address)) {
                return address.getHostAddress();
            }
        }
        return null;
    }
}
