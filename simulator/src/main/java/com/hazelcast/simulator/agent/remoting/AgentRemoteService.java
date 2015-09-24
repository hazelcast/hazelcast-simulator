package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;

public class AgentRemoteService {

    public static final String ANY_ADDRESS = "0.0.0.0";
    public static final int PORT = 9000;

    public enum Service {
        SERVICE_POKE,
        SERVICE_GET_FAILURES,
    }

    private static final Logger LOGGER = Logger.getLogger(AgentRemoteService.class);

    private final Agent agent;
    private final Executor executor = createFixedThreadPool(20, AgentRemoteService.class);

    private ServerSocket serverSocket;
    private AcceptorThread acceptorThread;

    public AgentRemoteService(Agent agent) {
        this.agent = agent;
    }

    public void start() throws IOException {
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
                        executor.execute(new ClientSocketTask(clientSocket, agent));
                    }
                } catch (IOException e) {
                    LOGGER.fatal("Exception in AcceptorThread", e);
                }
            }
        }
    }
}
