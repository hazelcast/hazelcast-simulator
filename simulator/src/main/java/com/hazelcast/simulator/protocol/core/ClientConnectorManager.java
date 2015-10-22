package com.hazelcast.simulator.protocol.core;

import com.hazelcast.simulator.protocol.connector.ClientConnector;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages outgoing client connections.
 */
public class ClientConnectorManager {

    private final ConcurrentHashMap<Integer, ClientConnector> clients = new ConcurrentHashMap<Integer, ClientConnector>();

    public void addClient(int clientIndex, ClientConnector clientConnector) {
        clients.put(clientIndex, clientConnector);
    }

    public void removeClient(int clientIndex) {
        ClientConnector clientConnector = clients.remove(clientIndex);
        if (clientConnector != null) {
            clientConnector.shutdown();
        }
    }

    public ClientConnector get(int clientIndex) {
        return clients.get(clientIndex);
    }

    public Collection<ClientConnector> getClientConnectors() {
        return clients.values();
    }
}
