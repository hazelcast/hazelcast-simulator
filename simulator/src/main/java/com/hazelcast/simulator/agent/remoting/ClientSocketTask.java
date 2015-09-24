package com.hazelcast.simulator.agent.remoting;

import org.apache.log4j.Logger;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

class ClientSocketTask implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ClientSocketTask.class);

    private final Socket clientSocket;

    ClientSocketTask(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            Object result;
            try {
                out = new ObjectOutputStream(clientSocket.getOutputStream());
                in = new ObjectInputStream(clientSocket.getInputStream());
                LOGGER.info("Poked by Coordinator");
                result = null;
            } catch (Exception e) {
                LOGGER.fatal("Exception in ClientSocketTask.run()", e);
                result = e;
            }
            if (out != null) {
                out.writeObject(result);
                out.flush();
            }
        } catch (Throwable e) {
            LOGGER.fatal("Exception in ClientSocketTask.run()", e);
        } finally {
            closeQuietly(in, out);
            closeQuietly(clientSocket);
        }
    }
}
