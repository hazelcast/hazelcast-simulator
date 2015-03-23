package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvm;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.worker.commands.Command;
import org.apache.log4j.Logger;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

class ClientSocketTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ClientSocketTask.class);

    private final Socket clientSocket;
    private final Agent agent;
    private final AgentMessageProcessor agentMessageProcessor;

    ClientSocketTask(Socket clientSocket, Agent agent, AgentMessageProcessor agentMessageProcessor) {
        this.clientSocket = clientSocket;
        this.agent = agent;
        this.agentMessageProcessor = agentMessageProcessor;
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
                LOGGER.fatal(e);
                result = e;
            }
            if (out != null) {
                out.writeObject(result);
                out.flush();
            }
        } catch (Throwable e) {
            LOGGER.fatal(e);
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
            case SERVICE_SPAWN_WORKERS:
                WorkerJvmSettings settings = (WorkerJvmSettings) in.readObject();
                spawnWorkers(settings);
                break;
            case SERVICE_INIT_TESTSUITE:
                TestSuite testSuite = (TestSuite) in.readObject();
                initTestSuite(testSuite);
                break;
            case SERVICE_TERMINATE_WORKERS:
                terminateWorkers();
                break;
            case SERVICE_EXECUTE_ALL_WORKERS:
                Command testCommand = (Command) in.readObject();
                WorkerJvmManager workerJvmManager = agent.getWorkerJvmManager();
                result = workerJvmManager.executeOnAllWorkers(testCommand);
                break;
            case SERVICE_EXECUTE_SINGLE_WORKER:
                testCommand = (Command) in.readObject();
                workerJvmManager = agent.getWorkerJvmManager();
                result = workerJvmManager.executeOnSingleWorker(testCommand);
                break;
            case SERVICE_ECHO:
                String msg = (String) in.readObject();
                echo(msg);
                break;
            case SERVICE_GET_FAILURES:
                result = getFailures();
                break;
            case SERVICE_GET_ALL_WORKERS:
                result = new ArrayList<String>();
                Collection<WorkerJvm> workerJvms = agent.getWorkerJvmManager().getWorkerJvms();
                for (WorkerJvm workerJvm : workerJvms) {
                    ((List<String>) result).add(workerJvm.id);
                }
                break;
            case SERVICE_PROCESS_MESSAGE:
                Message message = (Message) in.readObject();
                agentMessageProcessor.submit(message);
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

    private void spawnWorkers(WorkerJvmSettings settings) throws Exception {
        try {
            agent.getWorkerJvmManager().spawn(settings);
        } catch (Exception e) {
            LOGGER.fatal("Failed to spawn workers from settings:" + settings, e);
            throw e;
        }
    }

    private void initTestSuite(TestSuite testSuite) throws Exception {
        try {
            agent.initTestSuite(testSuite);
        } catch (Exception e) {
            LOGGER.fatal("Failed to init testsuite: " + testSuite, e);
            throw e;
        }
    }

    private void terminateWorkers() throws Exception {
        try {
            agent.getWorkerJvmManager().terminateWorkers();
        } catch (Exception e) {
            LOGGER.fatal("Failed to terminateWorker workers", e);
            throw e;
        }
    }

    private void echo(String msg) throws Exception {
        try {
            agent.echo(msg);
        } catch (Exception e) {
            LOGGER.fatal("Failed to echo", e);
            throw e;
        }
    }
}