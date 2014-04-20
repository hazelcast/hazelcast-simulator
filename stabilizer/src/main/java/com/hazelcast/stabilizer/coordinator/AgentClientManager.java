package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.WorkerJvmSettings;
import com.hazelcast.stabilizer.tests.Workout;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
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

import static java.lang.String.format;
import static javax.ws.rs.client.Entity.entity;

public class AgentClientManager {

    private final static ILogger log = Logger.getLogger(AgentClientManager.class);

    private final Coordinator coordinator;
    private List<AgentClient> agents = new LinkedList<AgentClient>();

    private final ExecutorService agentExecutor = Executors.newFixedThreadPool(100);

    public AgentClientManager(Coordinator coordinator, File machineListFile) {
        this.coordinator = coordinator;

        String content = Utils.asText(machineListFile);
        for (String line : content.split("\n")) {
            AgentClient client = new AgentClient(line);
            client.start();
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

    public void singleGenericTestTask(final String name) {
        if (agents.isEmpty()) {
            return;
        }

        Future f = agentExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                AgentClient agentClient = agents.get(0);
                agentClient.genericTestTask(name);
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

    //https://jersey.java.net/nonav/documentation/latest/user-guide.html
    public static class AgentClient {

        private final String host;
        private final String baseUrl;
        private WebTarget target;

        public AgentClient(String host) {
            this.host = host;
            this.baseUrl = format("http://%s:8080/", host);
        }

        public String getHost() {
            return host;
        }

        public void start() {
            Client c = ClientBuilder.newClient();
            target = c.target(baseUrl);
        }

        public void spawnWorkers(WorkerJvmSettings settings) {
            Response response = target
                    .path("agent/spawnWorkers")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity(settings, MediaType.APPLICATION_JSON));
            //System.out.println(response);
        }

        public void cleanWorkersHome() {
            Response response = target
                    .path("agent/cleanWorkersHome")
                    .request(MediaType.TEXT_PLAIN)
                    .post(null);
            //System.out.println(response);
        }

        public void genericTestTask(String methodName) {
            Response response = target
                    .path("agent/genericTestTask")
                    .request(MediaType.APPLICATION_JSON)
                    .put(entity(methodName, MediaType.TEXT_PLAIN_TYPE));
            //System.out.println(response);
        }

        public void echo(String msg) {
            Response response = target
                    .path("agent/echo")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity(msg, MediaType.TEXT_PLAIN_TYPE));
            //System.out.println(response);
        }

        public void prepareAgentForTest(TestRecipe testRecipe) {
            Response response = target
                    .path("agent/prepareForTest")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity(testRecipe, MediaType.APPLICATION_XML));
            //System.out.println(response);
        }

        public void initWorkout(Workout workout) {
            Response response = target
                    .path("agent/initWorkout")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity(workout, MediaType.APPLICATION_XML));
            //System.out.println(response);
        }

        public void initTest(TestRecipe testRecipe) {
            Response response = target
                    .path("agent/initTest")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity(testRecipe, MediaType.APPLICATION_XML));
            //System.out.println(response);
        }

        public void terminateWorkers() {
            Response response = target
                    .path("agent/terminateWorkers")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity("", MediaType.TEXT_PLAIN_TYPE));
            //System.out.println(response);
        }

        public void stopTest() {
            Response response = target
                    .path("agent/stopTest")
                    .request(MediaType.TEXT_PLAIN)
                    .put(entity("", MediaType.TEXT_PLAIN_TYPE));
            //System.out.println(response);
        }

        public List<Failure> getFailures() {
            List<Failure> response = target
                    .path("agent/failures")
                    .request(MediaType.APPLICATION_XML)
                    .get(new GenericType<List<Failure>>() {
                    });
            //System.out.println(response);
             return response;
        }
    }
}
