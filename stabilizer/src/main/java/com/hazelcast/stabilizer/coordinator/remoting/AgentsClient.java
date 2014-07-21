package com.hazelcast.stabilizer.coordinator.remoting;


import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.CountdownWatch;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.worker.commands.DoneCommand;
import com.hazelcast.stabilizer.worker.commands.Command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.agent.remoting.AgentRemoteService.Service.*;

public class AgentsClient {

    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(AgentsClient.class);

    private final List<AgentClient> agents = new LinkedList<AgentClient>();

    private final ExecutorService agentExecutor = Executors.newFixedThreadPool(100);

    public AgentsClient(List<AgentAddress> agentAddresses) {
        for (AgentAddress address : agentAddresses) {
            AgentClient client = new AgentClient(address);
            agents.add(client);
        }
    }

    public void awaitAgentsReachable() {
        List<AgentClient> unchecked = new LinkedList<AgentClient>(agents);

        log.info("--------------------------------------------------------------");
        log.info("Waiting for agents to start");
        log.info("--------------------------------------------------------------");

        for (int k = 0; k < 12; k++) {
            Iterator<AgentClient> it = unchecked.iterator();
            while (it.hasNext()) {
                AgentClient agent = it.next();
                try {
                    agent.execute(SERVICE_ECHO, "livecheck");
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
            log.info("--------------------------------------------------------------");
            Utils.exitWithError(log, "There are no reachable agents");
            log.info("--------------------------------------------------------------");
        }

        if (unchecked.isEmpty()) {
            log.info("--------------------------------------------------------------");
            log.info("All agents are reachable!");
            log.info("--------------------------------------------------------------");
            return;
        }

        StringBuilder sb = new StringBuilder("The Coordinator has dropped the following agents because they are not reachable:\n");
        for (AgentClient agent : unchecked) {
            sb.append("\t").append(agent.publicAddress).append("\n");
        }

        log.info("--------------------------------------------------------------");
        log.warning(sb.toString());
        log.info("--------------------------------------------------------------");
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
                    return agentClient.execute(SERVICE_GET_FAILURES);
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

    public void waitDone(String prefix,String testId) {
        long startTimeMs = System.currentTimeMillis();
        for (; ; ) {
            List<List<Boolean>> result = executeOnAllWorkers(new DoneCommand(testId));
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
                    agentClient.execute(SERVICE_INIT_TESTSUITE, testSuite);
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
                    agentClient.execute(SERVICE_TERMINATE_WORKERS);
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
                    agentClient.execute(SERVICE_SPAWN_WORKERS, settings);
                    return null;
                }
            });
            futures.add(f);

        }

        getAllFutures(futures);
    }

    public void sendMessage(final Message message) {
        log.info("Sending message '"+message+"' to address '"+message.getMessageAddress()+"'");
        MessageAddress messageAddress = message.getMessageAddress();
        List<Future> futures;
        if (MessageAddress.BROADCAST.equals(messageAddress.getAgentAddress())) {
            futures = sendMessageToAllAgents(message);
        } else if (MessageAddress.RANDOM.equals(messageAddress.getAgentAddress())) {
            Future future = sendMessageToRandomAgent(message);
            futures = Arrays.asList(future);
        } else {
            throw new UnsupportedOperationException("Not Implemented yet");
        }
        getAllFutures(futures);
    }

    private Future<Object> sendMessageToRandomAgent(final Message message) {
        Random random = new Random();
        final AgentClient agentClient = getRandomAgentClientOrNull(random);
        if (agentClient == null) {
            throw new IllegalStateException("No agent exists. Is this a race condition?");
        }
        return agentExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                agentClient.execute(SERVICE_PROCESS_MESSAGE, message);
                return null;
            }
        });
    }

    private AgentClient getRandomAgentClientOrNull(Random random) {
        if (agents.size() == 0) {
            return null;
        }
        return agents.get(random.nextInt(agents.size()));
    }

    private List<Future> sendMessageToAllAgents(final Message message) {
        List<Future> futures = new ArrayList<Future>();
        for (final AgentClient agentClient : agents) {
            Future<Object> future = agentExecutor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    agentClient.execute(SERVICE_PROCESS_MESSAGE, message);
                    return null;
                }
            });
            futures.add(future);
        }
        return futures;
    }

    public <E> List<String> getWorkers(final AgentClient agentClient) {
        Future future = agentExecutor.submit(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return (List<String>) agentClient.execute(SERVICE_GET_ALL_WORKERS);
            }
        });
        return getAllFutures(Arrays.asList(future));
    }


    public <E> List<E> executeOnAllWorkers(final Command command) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    try {
                        return agentClient.execute(SERVICE_EXECUTE_ALL_WORKERS, command);
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


    public void executeOnSingleWorker(final Command command) {
        if (agents.isEmpty()) {
            return;
        }

        Future f = agentExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                AgentClient agentClient = agents.get(0);
                return agentClient.execute(SERVICE_EXECUTE_SINGLE_WORKER, command);
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
                    agentClient.execute(SERVICE_ECHO, msg);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }
}
