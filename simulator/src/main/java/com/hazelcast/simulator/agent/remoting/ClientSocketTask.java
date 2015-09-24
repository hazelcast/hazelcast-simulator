package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor;
import com.hazelcast.simulator.test.Failure;
import org.apache.log4j.Logger;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

class ClientSocketTask implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ClientSocketTask.class);

    private final Socket clientSocket;
    private final Agent agent;

    ClientSocketTask(Socket clientSocket, Agent agent) {
        this.clientSocket = clientSocket;
        this.agent = agent;
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
                AgentRemoteService.Service service = (AgentRemoteService.Service) in.readObject();
                result = execute(service, in);
            } catch (Throwable e) {
                LOGGER.fatal("Exception in ClientSocketTask.run(): ", e);
                result = e;
            }
            if (out != null) {
                out.writeObject(result);
                out.flush();
            }
        } catch (Throwable e) {
            LOGGER.fatal("Exception in ClientSocketTask.run(): ", e);
        } finally {
            closeQuietly(in, out);
            closeQuietly(clientSocket);
        }
    }

    private Object execute(AgentRemoteService.Service service, ObjectInputStream in) throws Exception {
        Object result = null;
        switch (service) {
            case SERVICE_POKE:
                poke();
                break;
            case SERVICE_GET_FAILURES:
                result = getFailures();
                break;
            default:
                throw new RuntimeException("Unknown service:" + service);
        }
        return result;
    }

    private void poke() {
        LOGGER.info("Poked by coordinator");
    }

    private List<Failure> getFailures() {
        List<Failure> failures = new ArrayList<Failure>();
        WorkerJvmFailureMonitor failureMonitor = agent.getWorkerJvmFailureMonitor();
        failureMonitor.drainFailures(failures);
        return failures;
    }
}
