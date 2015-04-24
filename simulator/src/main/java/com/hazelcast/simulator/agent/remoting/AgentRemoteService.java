package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.Agent;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;

public class AgentRemoteService {

    public static final int PORT = 9000;
    public static final String ANY_ADDRESS = "0.0.0.0";

    public enum Service {
        SERVICE_SPAWN_WORKERS,
        SERVICE_INIT_TESTSUITE,
        SERVICE_TERMINATE_WORKERS,
        SERVICE_EXECUTE_ALL_WORKERS,
        SERVICE_EXECUTE_SINGLE_WORKER,
        SERVICE_ECHO,
        SERVICE_POKE,
        SERVICE_GET_FAILURES,
        SERVICE_GET_ALL_WORKERS,
        SERVICE_PROCESS_MESSAGE
    }

    private static final Logger LOGGER = Logger.getLogger(AgentRemoteService.class);

    private final Agent agent;
    private final AgentMessageProcessor agentMessageProcessor;
    private final Executor executor = createFixedThreadPool(20, AgentRemoteService.class);

    private ServerSocket serverSocket;
    private AcceptorThread acceptorThread;

    public AgentRemoteService(Agent agent, AgentMessageProcessor agentMessageProcessor) {
        this.agent = agent;
        this.agentMessageProcessor = agentMessageProcessor;
    }

    public void start() throws IOException {
        String bindAddress = (agent.getBindAddress() != null) ? agent.getBindAddress() : ANY_ADDRESS;
        serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(bindAddress));
        LOGGER.info("Started Agent Remote Service on: " + serverSocket.getInetAddress().getHostAddress() + ":" + PORT);

        acceptorThread = new AcceptorThread();
        acceptorThread.start();
    }

    public void stop() throws IOException {
        LOGGER.info("Stopping AgentRemoteService...");
        if (acceptorThread != null) {
            acceptorThread.shutdown();
        }
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    private class AcceptorThread extends Thread {

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
                        agent.signalUsed();
                        executor.execute(new ClientSocketTask(clientSocket, agent, agentMessageProcessor));
                    }
                } catch (IOException e) {
                    LOGGER.fatal("Exception in AcceptorThread", e);
                }
            }
        }
    }
}
