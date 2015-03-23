package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.agent.remoting.AgentRemoteService;
import com.hazelcast.simulator.common.AgentAddress;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSecondsThrowException;

public class AgentClient {

    private static final Logger LOGGER = Logger.getLogger(AgentClient.class);

    private final String publicAddress;
    private final String privateIp;

    public AgentClient(AgentAddress address) {
        this.publicAddress = address.publicAddress;
        this.privateIp = address.privateAddress;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public String getPrivateIp() {
        return privateIp;
    }

    @SuppressWarnings("unchecked")
    <E> E execute(AgentRemoteService.Service service, Object... args) throws Exception {
        Socket socket = newSocket();

        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(service);
            for (Object arg : args) {
                oos.writeObject(arg);
            }
            oos.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            E response = (E) in.readObject();

            if (response instanceof Exception) {
                Exception exception = (Exception) response;
                fixRemoteStackTrace(exception, Thread.currentThread().getStackTrace());
                throw exception;
            }
            return response;
        } finally {
            closeQuietly(socket);
        }
    }

    // we create a new socket for every request because it could be that the agents is not reachable
    // and we don't want to depend on state within the socket
    private Socket newSocket() throws IOException {
        ConnectException connectException = null;
        for (int i = 0; i < 30; i++) {
            try {
                InetAddress hostAddress = InetAddress.getByName(publicAddress);
                return new Socket(hostAddress, AgentRemoteService.PORT);
            } catch (ConnectException e) {
                if (i < 10) {
                    // it can happen that when a machine is under a lot of pressure, the connection can't be established
                    LOGGER.debug("Failed to connect to public address: " + publicAddress
                            + " sleeping for 1 second and trying again");
                } else {
                    LOGGER.warn("Failed to connect to public address: " + publicAddress
                            + " sleeping for 1 second and trying again");
                }
                sleepSecondsThrowException(1);
                connectException = e;
            } catch (IOException e) {
                throw new IOException("Couldn't connect to publicAddress: " + publicAddress
                        + ":" + AgentRemoteService.PORT, e);
            }
        }
        throw new IOException("Couldn't connect to publicAddress: " + publicAddress
                + ":" + AgentRemoteService.PORT, connectException);
    }

    @Override
    public String toString() {
        return "AgentClient{"
                + "publicAddress='" + publicAddress + '\''
                + ", privateIp='" + privateIp + '\''
                + '}';
    }
}
