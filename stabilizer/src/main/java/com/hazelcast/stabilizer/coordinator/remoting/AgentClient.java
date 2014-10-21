package com.hazelcast.stabilizer.coordinator.remoting;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.remoting.AgentRemoteService;
import com.hazelcast.stabilizer.common.AgentAddress;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import static com.hazelcast.stabilizer.Utils.closeQuietly;

public class AgentClient {

    final String publicAddress;
    final String privateIp;

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

    Object execute(AgentRemoteService.Service service, Object... args) throws Exception {
        Socket socket = newSocket();

        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(service);
            for (Object arg : args) {
                oos.writeObject(arg);
            }
            oos.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Object response = in.readObject();

            if (response instanceof Exception) {
                Exception exception = (Exception) response;
                Utils.fixRemoteStackTrace(exception, Thread.currentThread().getStackTrace());
                throw exception;
            }
            return response;
        } finally {
            closeQuietly(socket);
        }
    }

    //we create a new socket for every request because it could be that the agents is not reachable
    //and we don't want to depend on state within the socket.
    private Socket newSocket() throws IOException {
        try {
            InetAddress hostAddress = InetAddress.getByName(publicAddress);
            return new Socket(hostAddress, AgentRemoteService.PORT);
        } catch (IOException e) {
            throw new IOException("Couldn't connect to publicAddress: " + publicAddress + ":" + AgentRemoteService.PORT, e);
        }
    }

    @Override
    public String toString() {
        return "AgentClient{" +
                "publicAddress='" + publicAddress + '\'' +
                ", privateIp='" + privateIp + '\'' +
                '}';
    }
}