package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.agent.Agent;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.hazelcast.stabilizer.utils.CommonUtils.getHostAddress;

public class AgentRemoteService {

    public static final int PORT = 9000;

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

    private final static Logger log = Logger.getLogger(AgentRemoteService.class.getName());

    final private Agent agent;
    final private AgentMessageProcessor agentMessageProcessor;
    private ServerSocket serverSocket;
    private final Executor executor = Executors.newFixedThreadPool(20);
    private AcceptorThread acceptorThread;

    public AgentRemoteService(Agent agent, AgentMessageProcessor agentMessageProcessor) {
        this.agent = agent;
        this.agentMessageProcessor = agentMessageProcessor;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(getHostAddress()));
        log.info("Started Agent Remote Service on: " + serverSocket.getInetAddress().getHostAddress() + ":" + PORT);
        acceptorThread = new AcceptorThread();
        acceptorThread.start();
    }

    public void stop() throws IOException {
        acceptorThread.stopMe();
        serverSocket.close();
    }

    private class AcceptorThread extends Thread {
        private volatile boolean stopped = false;

        public AcceptorThread() {
            super("AcceptorThread");
        }

        public void stopMe() {
            stopped = true;
        }

        public void run() {
            while ( !stopped ) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    if (log.isDebugEnabled()) {
                        log.debug("Accepted coordinator request from: " + clientSocket.getRemoteSocketAddress());
                    }
                    agent.signalUsed();
                    executor.execute(new ClientSocketTask(clientSocket, agent, agentMessageProcessor));
                } catch (IOException e) {
                    log.fatal(e);
                }
            }
        }
    }
}