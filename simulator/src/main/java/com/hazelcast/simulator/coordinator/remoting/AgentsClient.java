package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.simulator.agent.remoting.AgentRemoteService;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.CountdownWatch;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.IsPhaseCompletedCommand;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_ECHO;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_EXECUTE_ALL_WORKERS;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_EXECUTE_SINGLE_WORKER;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_GET_FAILURES;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_INIT_TESTSUITE;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_POKE;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_PROCESS_MESSAGE;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_SPAWN_WORKERS;
import static com.hazelcast.simulator.agent.remoting.AgentRemoteService.Service.SERVICE_TERMINATE_WORKERS;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class AgentsClient {

    private static final Logger LOGGER = Logger.getLogger(AgentsClient.class);

    private static final long TEST_METHOD_TIMEOUT = TimeUnit.SECONDS.toMillis(Integer.parseInt(System.getProperty(
            "worker.testmethod.timeout", "10000")));

    private final ExecutorService agentExecutor = Executors.newFixedThreadPool(100);
    private final List<AgentClient> agents = new LinkedList<AgentClient>();

    private Thread pokeThread;

    public AgentsClient(List<AgentAddress> agentAddresses) {
        for (AgentAddress address : agentAddresses) {
            AgentClient client = new AgentClient(address);
            agents.add(client);
        }
    }

    public void start() {
        awaitAgentsReachable();

        // starts a poke thread which will repeatedly poke the agents to make sure they are not going to terminate themselves
        pokeThread = new Thread() {
            public void run() {
                for (; ; ) {
                    sleepSeconds(60);
                    asyncExecuteOnAllWorkers(SERVICE_POKE);
                }
            }
        };
        pokeThread.start();
    }

    public void stop() throws Exception {
        terminateWorkers();

        if (pokeThread != null) {
            pokeThread.interrupt();
            pokeThread.join();
        }

        agentExecutor.shutdown();
        agentExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void awaitAgentsReachable() {
        LOGGER.info("--------------------------------------------------------------");
        LOGGER.info("Waiting for agents to start");
        LOGGER.info("--------------------------------------------------------------");

        List<AgentClient> unchecked = new LinkedList<AgentClient>(agents);
        for (int i = 0; i < 12; i++) {
            Iterator<AgentClient> it = unchecked.iterator();
            while (it.hasNext()) {
                AgentClient agent = it.next();
                try {
                    agent.execute(SERVICE_ECHO, "livecheck");
                    it.remove();
                    LOGGER.info("Connect to agent " + agent.getPublicAddress() + " OK");
                } catch (Exception e) {
                    LOGGER.info("Connect to agent " + agent.getPublicAddress() + " FAILED");
                    LOGGER.debug(e);
                }
            }

            if (unchecked.isEmpty()) {
                break;
            }
            LOGGER.info("Sleeping 5 seconds and retrying unchecked agents");
            sleepSeconds(5);
        }

        agents.removeAll(unchecked);

        if (agents.isEmpty()) {
            exitWithError(LOGGER, "There are no reachable agents");
        }

        if (unchecked.isEmpty()) {
            LOGGER.info("--------------------------------------------------------------");
            LOGGER.info("All agents are reachable!");
            LOGGER.info("--------------------------------------------------------------");
            return;
        }

        StringBuilder sb = new StringBuilder("The Coordinator has dropped these agents because they are not reachable:\n");
        for (AgentClient agent : unchecked) {
            sb.append("\t").append(agent.getPublicAddress()).append("\n");
        }

        LOGGER.warn("--------------------------------------------------------------");
        LOGGER.warn(sb.toString());
        LOGGER.warn("--------------------------------------------------------------");
    }

    public int getAgentCount() {
        return agents.size();
    }

    public List<String> getPrivateAddresses() {
        List<String> result = new LinkedList<String>();
        for (AgentClient client : agents) {
            result.add(client.getPrivateIp());
        }
        return result;
    }

    public List<String> getPublicAddresses() {
        List<String> result = new LinkedList<String>();
        for (AgentClient client : agents) {
            result.add(client.getPublicAddress());
        }
        return result;
    }

    public List<Failure> getFailures() {
        List<Future<List<Failure>>> futures = new LinkedList<Future<List<Failure>>>();
        for (final AgentClient agentClient : agents) {
            Future<List<Failure>> future = agentExecutor.submit(new Callable<List<Failure>>() {
                @Override
                public List<Failure> call() throws Exception {
                    return agentClient.execute(SERVICE_GET_FAILURES);
                }
            });
            futures.add(future);
        }

        List<Failure> result = new LinkedList<Failure>();
        for (Future<List<Failure>> future : futures) {
            try {
                List<Failure> list = future.get(30, TimeUnit.SECONDS);
                result.addAll(list);
            } catch (InterruptedException e) {
                LOGGER.fatal(e);
            } catch (ExecutionException e) {
                LOGGER.fatal(e);
            } catch (TimeoutException e) {
                LOGGER.fatal(e);
            }
        }
        return result;
    }

    public void waitForPhaseCompletion(String prefix, String testId, String phaseName) throws TimeoutException {
        long startTimeMs = System.currentTimeMillis();
        IsPhaseCompletedCommand command = new IsPhaseCompletedCommand(testId);
        for (; ; ) {
            List<List<Boolean>> allResults = executeOnAllWorkers(command);
            boolean complete = true;
            for (List<Boolean> resultsPerAgent : allResults) {
                for (Boolean result : resultsPerAgent) {
                    if (!result) {
                        complete = false;
                        break;
                    }
                }

                if (!complete) {
                    break;
                }
            }

            if (complete) {
                return;
            }

            long durationMs = System.currentTimeMillis() - startTimeMs;
            LOGGER.info(prefix + "Waiting for " + phaseName + " completion: " + secondsToHuman(durationMs / 1000));
            sleepSeconds(5);
        }
    }

    public void initTestSuite(final TestSuite testSuite) throws TimeoutException {
        executeOnAllWorkers(SERVICE_INIT_TESTSUITE, testSuite);
    }

    public void terminateWorkers() throws TimeoutException {
        executeOnAllWorkers(SERVICE_TERMINATE_WORKERS);
    }

    public void spawnWorkers(List<AgentMemberLayout> agentLayouts, boolean member) throws TimeoutException {
        List<Future<Object>> futures = new LinkedList<Future<Object>>();

        for (AgentMemberLayout spawnPlan : agentLayouts) {
            final AgentClient agentClient = getAgent(spawnPlan.publicIp);
            if (agentClient == null) {
                throw new RuntimeException("agentClient is null!");
            }

            final WorkerJvmSettings settings;
            if (member) {
                settings = spawnPlan.memberSettings;
                if (spawnPlan.memberSettings.clientWorkerCount > 0) {
                    // TODO: remove
                    LOGGER.fatal("Found clients during member startup");
                }
            } else {
                settings = spawnPlan.clientSettings;
                if (spawnPlan.clientSettings.memberWorkerCount > 0) {
                    // TODO: remove
                    LOGGER.fatal("Found members during client startup");
                }
            }

            Future<Object> future = agentExecutor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    agentClient.execute(SERVICE_SPAWN_WORKERS, settings);
                    return null;
                }
            });
            futures.add(future);
        }

        getAllFutures(futures);
    }

    private AgentClient getAgent(String publicIp) {
        for (AgentClient client : agents) {
            if (publicIp.equals(client.getPublicAddress())) {
                return client;
            }
        }
        return null;
    }

    public void sendMessage(final Message message) throws TimeoutException {
        MessageAddress messageAddress = message.getMessageAddress();
        LOGGER.info("Sending message '" + message + "' to address '" + messageAddress + "'");

        if (MessageAddress.BROADCAST.equals(messageAddress.getAgentAddress())) {
            sendMessageToAllAgents(message);
        } else if (MessageAddress.RANDOM.equals(messageAddress.getAgentAddress())) {
            sendMessageToRandomAgent(message);
        } else {
            throw new UnsupportedOperationException("Not Implemented yet");
        }
    }

    private void sendMessageToAllAgents(Message message) throws TimeoutException {
        executeOnAllWorkers(SERVICE_PROCESS_MESSAGE, message);
    }

    private void sendMessageToRandomAgent(final Message message) throws TimeoutException {
        final AgentClient agentClient = getRandomAgentClientOrNull();
        if (agentClient == null) {
            throw new IllegalStateException("No agent exists. Is this a race condition?");
        }
        Future<Object> future = agentExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                agentClient.execute(SERVICE_PROCESS_MESSAGE, message);
                return null;
            }
        });
        getAllFutures(Collections.singletonList(future));
    }

    private AgentClient getRandomAgentClientOrNull() {
        if (agents.size() == 0) {
            return null;
        }
        Random random = new Random();
        return agents.get(random.nextInt(agents.size()));
    }

    public <E> List<E> executeOnAllWorkers(final Command command) throws TimeoutException {
        List<Future<E>> futures = new LinkedList<Future<E>>();
        for (final AgentClient agentClient : agents) {
            Future<E> future = agentExecutor.submit(new Callable<E>() {
                @Override
                public E call() throws Exception {
                    try {
                        return agentClient.execute(SERVICE_EXECUTE_ALL_WORKERS, command);
                    } catch (RuntimeException e) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(e.getMessage(), e);
                        } else {
                            LOGGER.fatal(e.getMessage());
                        }
                        throw e;
                    }
                }
            });
            futures.add(future);
        }

        return getAllFutures(futures);
    }

    // a temporary hack to get the correct mapping between futures and their agents
    public <E> Map<AgentClient, List<E>> executeOnAllWorkersDetailed(final Command command) throws TimeoutException {
        Map<AgentClient, Future<List<E>>> futures = new HashMap<AgentClient, Future<List<E>>>();

        for (final AgentClient agentClient : agents) {
            Future<List<E>> future = agentExecutor.submit(new Callable<List<E>>() {
                @Override
                public List<E> call() throws Exception {
                    try {
                        return agentClient.execute(SERVICE_EXECUTE_ALL_WORKERS, command);
                    } catch (RuntimeException e) {
                        LOGGER.fatal(e);
                        throw e;
                    }
                }
            });
            futures.put(agentClient, future);
        }

        Map<AgentClient, List<E>> resultMap = new HashMap<AgentClient, List<E>>();
        for (Map.Entry<AgentClient, Future<List<E>>> entry : futures.entrySet()) {
            AgentClient agentClient = entry.getKey();
            Future<List<E>> future = entry.getValue();
            List<List<E>> result = getAllFutures(Collections.singletonList(future));
            resultMap.put(agentClient, result.get(0));
        }

        return resultMap;
    }

    public void executeOnSingleWorker(final Command command) {
        if (agents.isEmpty()) {
            return;
        }

        Future<Object> future = agentExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                AgentClient agentClient = agents.get(0);
                return agentClient.execute(SERVICE_EXECUTE_SINGLE_WORKER, command);
            }
        });

        try {
            getAllFutures(Collections.singletonList(future));
        } catch (Throwable ignored) {
        }
    }

    public void echo(final String msg) throws TimeoutException {
        executeOnAllWorkers(SERVICE_ECHO, msg);
    }

    private void executeOnAllWorkers(AgentRemoteService.Service service, Object... args) throws TimeoutException {
        getAllFutures(asyncExecuteOnAllWorkers(service, args));
    }

    private List<Future<Object>> asyncExecuteOnAllWorkers(final AgentRemoteService.Service service, final Object... args) {
        List<Future<Object>> futures = new LinkedList<Future<Object>>();
        for (final AgentClient agentClient : agents) {
            Future<Object> future = agentExecutor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    agentClient.execute(service, args);
                    return null;
                }
            });
            futures.add(future);
        }
        return futures;
    }

    private <E> List<E> getAllFutures(Collection<Future<E>> futures) throws TimeoutException {
        return getAllFutures(futures, TEST_METHOD_TIMEOUT);
    }

    // TODO: probably we don't want to throw exceptions to make sure that don't abort when an agent goes down
    private <E> List<E> getAllFutures(Collection<Future<E>> futures, long timeoutMs) throws TimeoutException {
        CountdownWatch watch = CountdownWatch.started(timeoutMs);
        List<E> resultList = new LinkedList<E>();
        for (Future<E> future : futures) {
            try {
                E result = future.get(watch.getRemainingMs(), TimeUnit.MILLISECONDS);
                resultList.add(result);
            } catch (TimeoutException e) {
                //Failure failure = new Failure();
                //failure.message = "Timeout waiting for remote operation to complete";
                //failure.agentAddress = getHostAddress();
                //failure.testRecipe = console.getTestRecipe();
                //failure.cause = e;
                //console.statusTopic.publish(failure);

                throw e;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (!(cause instanceof FailureAlreadyThrownRuntimeException)) {
                    //Failure failure = new Failure();
                    //failure.agentAddress = getHostAddress();
                    //failure.testRecipe = console.getTestRecipe();
                    //failure.cause = e;
                    //console.statusTopic.publish(failure);
                }

                fixRemoteStackTrace(cause, Thread.currentThread().getStackTrace());

                if (cause instanceof TimeoutException) {
                    throw (TimeoutException) cause;
                }

                if (cause instanceof RuntimeException) {
                    throw ((RuntimeException) cause);
                }

                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return resultList;
    }
}
