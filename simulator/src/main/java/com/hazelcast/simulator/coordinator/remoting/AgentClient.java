package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.protocol.registry.AgentData;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;

import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.PORT;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSecondsThrowException;
import static java.lang.String.format;

class AgentClient {

    private static final int CREATE_SOCKET_MAX_RETRIES = 30;
    private static final int LOG_FAILURE_AS_WARNING_THRESHOLD = 10;

    private static final Logger LOGGER = Logger.getLogger(AgentClient.class);

    private final String publicAddress;

    public AgentClient(AgentData agentData) {
        this.publicAddress = agentData.getPublicAddress();
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    @SuppressWarnings("unchecked")
    <E> E execute(Object... args) throws Exception {
        Socket socket = newSocket();

        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
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
        for (int i = 0; i < CREATE_SOCKET_MAX_RETRIES; i++) {
            try {
                InetAddress hostAddress = InetAddress.getByName(publicAddress);
                return new Socket(hostAddress, PORT);
            } catch (ConnectException e) {
                String logMessage = format("Failed to connect to public address %s:%s, sleeping for 1 second and trying again",
                        publicAddress, PORT);
                if (i < LOG_FAILURE_AS_WARNING_THRESHOLD) {
                    // it can happen that when a machine is under a lot of pressure, the connection can't be established
                    LOGGER.debug(logMessage);
                } else {
                    LOGGER.warn(logMessage);
                }
                sleepSecondsThrowException(1);
                connectException = e;
            } catch (IOException e) {
                throw new IOException(format("Couldn't connect to publicAddress %s:%s", publicAddress, PORT), e);
            }
        }
        throw new IOException(format("Couldn't connect to publicAddress %s:%s", publicAddress, PORT), connectException);
    }
}
