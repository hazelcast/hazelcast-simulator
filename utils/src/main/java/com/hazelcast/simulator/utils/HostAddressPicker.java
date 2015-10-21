package com.hazelcast.simulator.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public final class HostAddressPicker {

    private HostAddressPicker() {
    }

    public static String pickHostAddress() {
        Exception savedException = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            String loopbackHost = null;
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                try {
                    if (!checkInterface(networkInterface)) {
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
                } catch (SocketException e) {
                    savedException = getException(networkInterface, e);
                }
            }
            if (loopbackHost != null) {
                return loopbackHost;
            } else {
                if (savedException == null) {
                    throw new IllegalStateException("Cannot find local host address");
                }
                throw new IllegalStateException("Cannot find local host address", savedException);
            }
        } catch (SocketException e) {
            throw rethrow(e);
        }
    }

    private static boolean checkInterface(NetworkInterface networkInterface) throws SocketException {
        if (!networkInterface.isUp()) {
            return false;
        }
        if (networkInterface.isPointToPoint()) {
            return false;
        }
        return true;
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

    private static Exception getException(NetworkInterface networkInterface, SocketException e) {
        Exception savedException;
        StringBuilder sb = new StringBuilder("Error during pickHostAddress()");
        sb.append(NEW_LINE).append("displayName: ").append(networkInterface.getDisplayName());
        sb.append(NEW_LINE).append("name: ").append(networkInterface.getName());
        for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
            sb.append(NEW_LINE).append("interfaceAddress: ").append(interfaceAddress.getAddress());
        }
        savedException = new IllegalStateException(sb.toString(), e);
        return savedException;
    }
}
