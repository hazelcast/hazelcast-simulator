package com.hazelcast.stabilizer.coordinator;


import com.hazelcast.logging.ILogger;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.AgentRemoteService;
import com.hazelcast.stabilizer.agent.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.agent.WorkerJvmSettings;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.TestCommand;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AgentClientManager {

    private final static ILogger log = com.hazelcast.logging.Logger.getLogger(AgentClientManager.class.getName());

    private final Coordinator coordinator;
    private List<AgentClient> agents = new LinkedList<AgentClient>();

    private final ExecutorService agentExecutor = Executors.newFixedThreadPool(100);

    public AgentClientManager(Coordinator coordinator, File machineListFile) {
        this.coordinator = coordinator;

        String content = Utils.asText(machineListFile);
        for (String line : content.split("\n")) {
            AgentClient client = new AgentClient(line);
            agents.add(client);
        }
    }

    public int getAgentCount() {
        return agents.size();
    }

    public List<String> getHostAddresses() {
        List<String> result = new LinkedList();
        for (AgentClient client : agents) {
            result.add(client.getHost());
        }
        return result;
    }

    public List<Failure> getFailures() {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    return agentClient.getFailures();
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

    public void prepareAgentsForTests(final TestRecipe testRecipe) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.prepareAgentForTest(testRecipe);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void cleanWorkersHome() {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.cleanWorkersHome();
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    private void getAllFutures(Collection<Future> futures) {
        getAllFutures(futures, TimeUnit.SECONDS.toMillis(10000));
    }

    private void getAllFutures(Collection<Future> futures, long timeoutMs) {
        for (Future future : futures) {
            try {
                //todo: we should calculate remaining timeoutMs
                Object o = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
//                Failure failure = new Failure();
//                failure.message = "Timeout waiting for remote operation to complete";
//                failure.agentAddress = getHostAddress();
//                failure.testRecipe = console.getTestRecipe();
//                failure.cause = e;
//                console.statusTopic.publish(failure);
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof FailureAlreadyThrownRuntimeException)) {
//                    Failure failure = new Failure();
//                    failure.agentAddress = getHostAddress();
//                    failure.testRecipe = console.getTestRecipe();
//                    failure.cause = e;
//                    console.statusTopic.publish(failure);
                }
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void initWorkout(final Workout workout, byte[] bytes) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.initWorkout(workout);
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
                    agentClient.terminateWorkers();
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void spawnWorkers(final WorkerJvmSettings workerJvmSettings) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.spawnWorkers(workerJvmSettings);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void testCommand(final TestCommand testCommand) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    try {
                        agentClient.testCommand(testCommand);
                        return null;
                    } catch (RuntimeException t) {
                        log.severe(t);
                        throw t;
                    }
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void singleGenericTestTask(final String name) {
        if (agents.isEmpty()) {
            return;
        }

        Future f = agentExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                AgentClient agentClient = agents.get(0);
                GenericTestCommand testCommand = new GenericTestCommand(name);
                //todo: the problem here is that you are going to ask all member and not a single member.
                agentClient.testCommand(testCommand);
                return null;
            }
        });

        getAllFutures(Arrays.asList(f));
    }

    public void echo(final String msg) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.echo(msg);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public static class AgentClient {

        private final String host;

        public AgentClient(String host) {
            this.host = host;
        }

        public String getHost() {
            return host;
        }

        private Object execute(String service, Object... args) throws Exception {
            Socket socket = new Socket(InetAddress.getByName(host), 9000);

            try {
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(service);
                for (Object arg : args) {
                    oos.writeObject(arg);
                }
                oos.flush();

                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                return in.readObject();
            } finally {
                Utils.closeQuietly(socket);
            }
        }

        public void spawnWorkers(WorkerJvmSettings settings) throws Exception {
            execute(AgentRemoteService.SERVICE_SPAWN_WORKERS, settings);
        }

        public void cleanWorkersHome() throws Exception {
            execute(AgentRemoteService.SERVICE_CLEAN_WORKERS_HOME);
        }

        public void testCommand(TestCommand testCommand) throws Exception {
            execute(AgentRemoteService.SERVICE_TEST_COMMAND, testCommand);
        }

        public void echo(String msg) throws Exception {
            execute(AgentRemoteService.SERVICE_ECHO, msg);
        }

        public void prepareAgentForTest(TestRecipe testRecipe) throws Exception {
            execute(AgentRemoteService.SERVICE_PREPARE_FOR_TEST, testRecipe);
        }

        public void initWorkout(Workout workout) throws Exception {
            execute(AgentRemoteService.SERVICE_INIT_WORKOUT, workout);
        }

        public void terminateWorkers() throws Exception {
            execute(AgentRemoteService.SERVICE_TERMINATE_WORKERS);
        }

        public List<Failure> getFailures() throws Exception {
            return (List<Failure>) execute(AgentRemoteService.SERVICE_GET_FAILURES);
        }
    }
}
