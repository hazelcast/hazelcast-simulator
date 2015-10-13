package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;

public class AgentRemoteService {

    public static final int PORT = 9000;

    private static final String ANY_ADDRESS = "0.0.0.0";

    private static final Logger LOGGER = Logger.getLogger(AgentRemoteService.class);

    private final Executor executor = createFixedThreadPool(1, AgentRemoteService.class);

    private final ServerSocket serverSocket;
    private final AcceptorThread acceptorThread;

    public AgentRemoteService() throws Exception {
        serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(ANY_ADDRESS));
        LOGGER.info("Started Agent Remote Service on: " + serverSocket.getInetAddress().getHostAddress() + ":" + PORT);

        acceptorThread = new AcceptorThread();
        acceptorThread.start();
    }

    public void shutdown() {
        LOGGER.info("Stopping AgentRemoteService...");
        if (acceptorThread != null) {
            acceptorThread.shutdown();
        }
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            EmptyStatement.ignore(e);
        }
    }

    private final class AcceptorThread extends Thread {

        private volatile boolean running = true;

        private AcceptorThread() {
            super("AcceptorThread");
        }

        private void shutdown() {
            running = false;
        }

        public void run() {
            while (running) {
                try {
                    if (!serverSocket.isClosed()) {
                        Socket clientSocket = serverSocket.accept();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Accepted coordinator request from: " + clientSocket.getRemoteSocketAddress());
                        }
                        executor.execute(new ClientSocketTask(clientSocket));
                    }
                } catch (IOException e) {
                    LOGGER.fatal("Exception in AcceptorThread", e);
                }
            }
        }
    }
}
