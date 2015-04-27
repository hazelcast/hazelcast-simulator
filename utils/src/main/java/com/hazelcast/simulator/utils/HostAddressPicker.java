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
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            String loopbackHost = null;
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (!iface.isUp() || iface.isPointToPoint()) {
                    continue;
                }
                if (!iface.isLoopback()) {
                    String candidateAddress = getHostAddressOrNull(iface);
                    if (candidateAddress != null) {
                        return candidateAddress;
                    }
                } else {
                    if (loopbackHost == null) {
                        loopbackHost = getHostAddressOrNull(iface);
                    }
                }
            }
            if (loopbackHost != null) {
                return loopbackHost;
            } else {
                throw new IllegalStateException("Cannot find local host address");
            }
        } catch (SocketException e) {
            throw CommonUtils.rethrow(e);
        }
    }

    private static String getHostAddressOrNull(NetworkInterface iface) {
        Enumeration<InetAddress> addresses = iface.getInetAddresses();
        while (addresses.hasMoreElements()) {
            InetAddress address = addresses.nextElement();
            if (!(address instanceof Inet6Address)) {
                return address.getHostAddress();
            }

        }
        return null;
    }
}
