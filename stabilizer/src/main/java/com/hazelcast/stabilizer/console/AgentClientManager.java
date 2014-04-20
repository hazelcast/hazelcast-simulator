package com.hazelcast.stabilizer.console;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.agent.WorkerJvmSettings;

import java.io.File;
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

    private final static ILogger log = Logger.getLogger(AgentClientManager.class);

    private final Console console;
    private List<AgentClient> agents = new LinkedList<AgentClient>();

    private final ExecutorService agentExecutor = Executors.newFixedThreadPool(100);

    public AgentClientManager(Console console, File machineListFile) {
        this.console = console;

        String content = Utils.asText(machineListFile);
        for (String line : content.split("\n")) {
            AgentClient client = new AgentClient(line);
            client.start();
            agents.add(client);
        }
    }

    public int getAgentCount(){
        return agents.size();
    }

    public List<String> getHostAddresses(){
        List<String> result = new LinkedList();
        for(AgentClient client: agents){
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

    public void initTest(final TestRecipe testRecipe) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    try {
                        agentClient.initTest(testRecipe);
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

    public void globalGenericTestTask(final String name) {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.genericTestTask(name);
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }

    public void singleGenericTestTask(String name) {
        System.out.println();
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

    public void stopTest() {
        List<Future> futures = new LinkedList<Future>();
        for (final AgentClient agentClient : agents) {
            Future f = agentExecutor.submit(new Callable() {
                @Override
                public Object call() throws Exception {
                    agentClient.stopTest();
                    return null;
                }
            });
            futures.add(f);
        }

        getAllFutures(futures);
    }
}
