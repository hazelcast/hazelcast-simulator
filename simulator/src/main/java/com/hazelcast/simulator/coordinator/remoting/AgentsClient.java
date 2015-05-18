package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.agent.remoting.AgentRemoteService;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.CountdownWatch;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
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
import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSecondsThrowException;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;

public class AgentsClient {

    private static final Logger LOGGER = Logger.getLogger(AgentsClient.class);

    private static final long TEST_METHOD_TIMEOUT = TimeUnit.SECONDS.toMillis(Integer.parseInt(System.getProperty(
            "worker.testmethod.timeout", "10000")));

    private final ExecutorService agentExecutor = createFixedThreadPool(100, AgentsClient.class);
    private final List<AgentClient> agents = new LinkedList<AgentClient>();
    private final Random random = new Random();

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
                    try {
                        sleepSecondsThrowException(60);
                        asyncExecuteOnAllWorkers(SERVICE_POKE);
                    } catch (RuntimeException e) {
                        if (e.getCause() instanceof InterruptedException) {
                            break;
                        }
                        throw e;
                    }
                }
            }
        };
        pokeThread.start();
    }

    private void awaitAgentsReachable() {
        LOGGER.info("--------------------------------------------------------------");
        LOGGER.info("Waiting for agents to start");
        LOGGER.info("--------------------------------------------------------------");

        List<AgentClient> uncheckedAgents = new LinkedList<AgentClient>(agents);
        for (int i = 0; i < 12; i++) {
            Iterator<AgentClient> agentIterator = uncheckedAgents.iterator();
            while (agentIterator.hasNext()) {
                AgentClient agent = agentIterator.next();
                try {
                    agent.execute(SERVICE_ECHO, "livecheck");
                    agentIterator.remove();
                    LOGGER.info("Connect to agent " + agent.getPublicAddress() + " OK");
                } catch (Exception e) {
                    LOGGER.info("Connect to agent " + agent.getPublicAddress() + " FAILED");
                    LOGGER.debug(e);
                }
            }

            if (uncheckedAgents.isEmpty()) {
                break;
            }
            LOGGER.info("Sleeping 5 seconds and retrying unchecked agents");
            sleepSeconds(5);
        }

        if (!uncheckedAgents.isEmpty()) {
            StringBuilder sb = new StringBuilder("The Coordinator has dropped these agents because they are not reachable:\n");
            for (AgentClient agent : uncheckedAgents) {
                sb.append("\t").append(agent.getPublicAddress()).append("\n");
            }

            LOGGER.warn("--------------------------------------------------------------");
            LOGGER.warn(sb.toString());
            LOGGER.warn("--------------------------------------------------------------");

            agents.removeAll(uncheckedAgents);
            if (agents.isEmpty()) {
                throw new CommandLineExitException("There are no reachable agents");
            }
        } else {
            LOGGER.info("--------------------------------------------------------------");
            LOGGER.info("All agents are reachable!");
            LOGGER.info("--------------------------------------------------------------");
        }
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

    public int getAgentCount() {
        return agents.size();
    }

    public List<String> getPublicAddresses() {
        List<String> result = new LinkedList<String>();
        for (AgentClient client : agents) {
            result.add(client.getPublicAddress());
        }
        return result;
    }

    public List<String> getPrivateAddresses() {
        List<String> result = new LinkedList<String>();
        for (AgentClient client : agents) {
            result.add(client.getPrivateIp());
        }
        return result;
    }

    public List<Failure> getFailures() {
        List<Future<List<Failure>>> futures = asyncExecuteOnAllWorkers(SERVICE_GET_FAILURES);
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

    public void initTestSuite(TestSuite testSuite) throws TimeoutException {
        executeOnAllWorkers(SERVICE_INIT_TESTSUITE, testSuite);
    }

    public void waitForPhaseCompletion(String prefix, String testId, TestPhase testPhase) throws TimeoutException {
        long start = System.nanoTime();
        IsPhaseCompletedCommand command = new IsPhaseCompletedCommand(testId, testPhase);
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
            long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            LOGGER.info(prefix + "Waiting " + secondsToHuman(duration) + " for " + testPhase.name + " completion");
            sleepSeconds(5);
        }
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

    private AgentClient getAgent(String publicAddress) {
        for (AgentClient client : agents) {
            if (publicAddress.equals(client.getPublicAddress())) {
                return client;
            }
        }
        return null;
    }

    public void terminateWorkers() throws TimeoutException {
        executeOnAllWorkers(SERVICE_TERMINATE_WORKERS);
    }

    public void sendMessage(Message message) throws TimeoutException {
        MessageAddress messageAddress = message.getMessageAddress();
        LOGGER.info("Sending message '" + message + "' to address '" + messageAddress + "'");

        String agentAddress = messageAddress.getAgentAddress();
        if (MessageAddress.BROADCAST.equals(agentAddress)) {
            executeOnAllWorkers(SERVICE_PROCESS_MESSAGE, message);
        } else if (MessageAddress.RANDOM.equals(agentAddress)) {
            executeOnRandomWorker(SERVICE_PROCESS_MESSAGE, message);
        } else {
            throw new UnsupportedOperationException("Unsupported message address: " + agentAddress);
        }
    }

    public void echo(String msg) {
        try {
            executeOnAllWorkers(SERVICE_ECHO, msg);
        } catch (TimeoutException e) {
            LOGGER.warn("Failed to send echo message to agents due to timeout");
        }
    }

    public void executeOnFirstWorker(final Command command) throws TimeoutException {
        Future<Object> future = asyncExecuteOnWorker(0, SERVICE_EXECUTE_SINGLE_WORKER, command);
        getAllFutures(Collections.singletonList(future));
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

    private void executeOnRandomWorker(final AgentRemoteService.Service service, final Object... args) throws TimeoutException {
        Future<Object> future = asyncExecuteOnWorker(random.nextInt(agents.size()), service, args);
        getAllFutures(Collections.singletonList(future));
    }

    private void executeOnAllWorkers(AgentRemoteService.Service service, Object... args) throws TimeoutException {
        getAllFutures(asyncExecuteOnAllWorkers(service, args));
    }

    private <E> List<Future<E>> asyncExecuteOnAllWorkers(final AgentRemoteService.Service service, final Object... args) {
        List<Future<E>> futures = new LinkedList<Future<E>>();
        for (final AgentClient agentClient : agents) {
            Future<E> future = agentExecutor.submit(new Callable<E>() {
                @Override
                public E call() throws Exception {
                    return agentClient.execute(service, args);
                }
            });
            futures.add(future);
        }
        return futures;
    }

    private <E> Future<E> asyncExecuteOnWorker(int agentIndex, final AgentRemoteService.Service service, final Object... args) {
        if (agents.size() == 0) {
            throw new IllegalStateException("No agent exists. Is this a race condition?");
        }
        final AgentClient agentClient = agents.get(agentIndex);
        return agentExecutor.submit(new Callable<E>() {
            @Override
            public E call() throws Exception {
                return agentClient.execute(service, args);
            }
        });
    }

    // TODO: probably we don't want to throw exceptions to make sure that don't abort when an agent goes down
    private <E> List<E> getAllFutures(Collection<Future<E>> futures) throws TimeoutException {
        CountdownWatch watch = CountdownWatch.started(TEST_METHOD_TIMEOUT);
        List<E> resultList = new LinkedList<E>();
        for (Future<E> future : futures) {
            try {
                E result = future.get(watch.getRemainingMs(), TimeUnit.MILLISECONDS);
                resultList.add(result);
            //} catch (TimeoutException e) {
                //Failure failure = new Failure();
                //failure.message = "Timeout waiting for remote operation to complete";
                //failure.agentAddress = getHostAddress();
                //failure.testRecipe = console.getTestRecipe();
                //failure.cause = e;
                //console.statusTopic.publish(failure);
                //throw e;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                //if (!(cause instanceof FailureAlreadyThrownRuntimeException)) {
                //Failure failure = new Failure();
                //failure.agentAddress = getHostAddress();
                //failure.testRecipe = console.getTestRecipe();
                //failure.cause = e;
                //console.statusTopic.publish(failure);
                //}
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
