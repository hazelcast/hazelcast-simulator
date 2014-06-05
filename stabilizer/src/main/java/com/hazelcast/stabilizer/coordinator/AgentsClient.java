package com.hazelcast.stabilizer.coordinator;


import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.AgentRemoteService;
import com.hazelcast.stabilizer.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.common.CountdownWatch;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.worker.testcommands.DoneCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommand;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.stabilizer.Utils.closeQuietly;
import static com.hazelcast.stabilizer.Utils.sleepSeconds;

public class AgentsClient {

    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(AgentsClient.class);

    private final List<AgentClient> agents = new LinkedList<AgentClient>();

    private final ExecutorService agentExecutor = Executors.newFixedThreadPool(100);

    public AgentsClient(File agentFile) {
        for (AgentAddress address : AgentsFile.load(agentFile)) {
            AgentClient client = new AgentClient(address);
            agents.add(client);
        }
    }

    public void awaitAgentsReachable() {
        List<AgentClient> unchecked = new LinkedList<AgentClient>(agents);

        echo("--------------------------------------------------------------");
        log.info("Waiting for agents to start");
        echo("--------------------------------------------------------------");

        for (int k = 0; k < 12; k++) {
            Iterator<AgentClient> it = unchecked.iterator();
            while (it.hasNext()) {
                AgentClient agent = it.next();
                try {
                    agent.execute(AgentRemoteService.SERVICE_ECHO, "livecheck");
                    it.remove();
                    log.info("Connect to agent " + agent.publicAddress + " OK");
                } catch (Exception e) {
                    log.info("Connect to agent " + agent.publicAddress + " FAILED");
                    log.finest(e);
                }
            }

            if (unchecked.isEmpty()) {
                break;
            }
            log.info("Sleeping 5 seconds and retrying unchecked agents");
            sleepSeconds(5);
        }

        agents.removeAll(unchecked);

        if (agents.isEmpty()) {
            echo("--------------------------------------------------------------");
            Utils.exitWithError(log, "There are no reachable agents");
            echo("--------------------------------------------------------------");
        }

        if (unchecked.isEmpty()) {
            echo("--------------------------------------------------------------");
            log.info("All agents are reachable!");
            echo("--------------------------------------------------------------");
            return;
        }

        StringBuilder sb = new StringBuilder("The Coordinator has dropped the following agents because they are not reachable:\n");
        for (AgentClient agent : unchecked) {
            sb.append("\t").append(agent.publicAddress).append("\n");
        }

        echo("--------------------------------------------------------------");
        log.warning(sb.toString());
        echo("--------------------------------------------------------------");
    }

    public int getAgentCount() {
        return agents.size();
    }

    public List<String> getPrivateAddresses() {
        List<String> result = new LinkedList<String>();
        for (AgentClient client : agents) {
            result.add(client.privateIp);
        }
        return result;
    }

    public List<String> getPublicAddresses() {
        List<String> result = new LinkedList<String>();
        for (AgentClient client : agents) {
            result.add(client.publicAddress);
        }
        return result;
    }

    public List<Failure> getFailures() {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    return agentClient.execute(AgentRemoteService.SERVICE_GET_FAILURES);
                }
            });
            futures.add(f);
        }

        List<Failure> result = new LinkedList<Failure>();
        for (Future<List<Failure>> f : futures) {
            try {
                List<Failure> c = f.get(30, TimeUnit.SECONDS);
                result.addAll(c);
            } catch (InterruptedException e) {
                log.severe(e);
            } catch (ExecutionException e) {
                log.severe(e);
            } catch (TimeoutException e) {
                log.severe(e);
            }
        }
        return result;
    }

    public void waitDone(String prefix) {
        long startTimeMs = System.currentTimeMillis();
        for (; ; ) {
            List<List<Boolean>> result = executeOnAllWorkers(new DoneCommand());
            boolean complete = true;
            for (List<Boolean> l : result) {
                for (Boolean b : l) {
                    if (!b) {
                        complete = false;
                        break;
                    }
                }
            }

            if (complete) {
                return;
            }

            long durationMs = System.currentTimeMillis() - startTimeMs;
            log.info(prefix+"Waiting for completion: " + Utils.secondsToHuman(durationMs / 1000));
            Utils.sleepSeconds(5);
        }
    }

    private <E> List<E> getAllFutures(Collection<Future> futures) {
        int value = Integer.parseInt(System.getProperty("worker.testmethod.timeout", "10000"));
        return getAllFutures(futures, TimeUnit.SECONDS.toMillis(value));
    }

    //todo: probably we don't want to throw exceptions to make sure that don't abort when a agent goes down.
    private <E> List<E> getAllFutures(Collection<Future> futures, long timeoutMs) {
        CountdownWatch watch = CountdownWatch.started(timeoutMs);
        List result = new LinkedList();
        for (Future future : futures) {
            try {
                Object o = future.get(watch.getRemainingMs(), TimeUnit.MILLISECONDS);
                result.add(o);
            } catch (TimeoutException e) {
//                Failure failure = new Failure();
//                failure.message = "Timeout waiting for remote operation to complete";
//                failure.agentAddress = getHostAddress();
//                failure.testRecipe = console.getTestRecipe();
//                failure.cause = e;
//                console.statusTopic.publish(failure);
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (!(cause instanceof FailureAlreadyThrownRuntimeException)) {
//                    Failure failure = new Failure();
//                    failure.agentAddress = getHostAddress();
//                    failure.testRecipe = console.getTestRecipe();
//                    failure.cause = e;
//                    console.statusTopic.publish(failure);
                }

                Utils.fixRemoteStackTrace(cause, Thread.currentThread().getStackTrace());
                if (cause instanceof RuntimeException) {
                    throw ((RuntimeException) cause);
                }

                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return result;
    }

    public void initTestSuite(final TestSuite testSuite) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.execute(AgentRemoteService.SERVICE_INIT_TESTSUITE, testSuite);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void terminateWorkers() {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.execute(AgentRemoteService.SERVICE_TERMINATE_WORKERS);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void spawnWorkers(final WorkerJvmSettings[] workerJvmSettingsArray) {
        List<Future> futures = new LinkedList<Future>();

        for (int k = 0; k < agents.size(); k++) {
            final int index = k;
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    AgentClient agentClient = agents.get(index);
                    WorkerJvmSettings settings = workerJvmSettingsArray[index];
                    agentClient.execute(AgentRemoteService.SERVICE_SPAWN_WORKERS, settings);
                    return null;
                }
            });
            futures.add(f);

        }

        getAllFutures(futures);
    }

    public <E> List<E> executeOnAllWorkers(final TestCommand testCommand) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    try {
                        return agentClient.execute(AgentRemoteService.SERVICE_EXECUTE_ALL_WORKERS, testCommand);
                    } catch (RuntimeException t) {
                        log.severe(t);
                        throw t;
                    }
                }
            });
            futures.add(f);
        }

        return getAllFutures(futures);
    }


    public void executeOnSingleWorker(final TestCommand testCommand) {
        if (agents.isEmpty()) {
            return;
        }

        Future f = agentExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                AgentClient agentClient = agents.get(0);
                return agentClient.execute(AgentRemoteService.SERVICE_EXECUTE_SINGLE_WORKER, testCommand);
            }
        });

        try {
            getAllFutures(Arrays.asList(f));
        } catch (Throwable e) {
            //ignore
        }
    }

    public void echo(final String msg) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.execute(AgentRemoteService.SERVICE_ECHO, msg);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    private static class AgentClient {

        final String publicAddress;
        final String privateIp;

        public AgentClient(AgentAddress address) {
            this.publicAddress = address.publicAddress;
            this.privateIp = address.privateAddress;
        }

        private Object execute(String service, Object... args) throws Exception {
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
    }
}
