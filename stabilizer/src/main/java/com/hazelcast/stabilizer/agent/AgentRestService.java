package com.hazelcast.stabilizer.agent;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.worker.tasks.GenericTestTask;
import com.hazelcast.stabilizer.worker.tasks.InitTest;
import com.hazelcast.stabilizer.worker.tasks.StopTask;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;

import static java.lang.String.format;

@Path("agent")
public class AgentRestService {
    private final static ILogger log = Logger.getLogger(AgentRestService.class);

    private Agent agent;

    public AgentRestService(Agent agent) {
        this.agent = agent;
    }

    @GET
    @Produces(MediaType.APPLICATION_XML)
    @Path("/failures")
    public ArrayList<Failure> getFailures() {
        ArrayList<Failure> failures = new ArrayList<Failure>();
        agent.getFailureMonitor().drainFailures(failures);
        return failures;
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/spawnWorkers")
    public String spawnWorkers(WorkerJvmSettings settings) throws Exception {
        try {
            agent.getWorkerJvmManager().spawn(settings);
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to spawn workers",e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/initTest")
    public String initTest(TestRecipe testRecipe) throws Exception {
        try {
            InitTest initTest = new InitTest(testRecipe);
            agent.getWorkerJvmManager().executeOnWorkers(initTest, "Test Initializing");
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to init Test", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/initWorkout")
    public String initWorkout(Workout workout) throws Exception {
        try {
            agent.initWorkout(workout, null);
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to init workout", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/cleanWorkersHome")
    public String cleanWorkersHome() throws Exception {
        try {
            agent.getWorkerJvmManager().cleanWorkersHome();
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to clean workers home", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/terminateWorkers")
    public String terminateWorkers() throws Exception {
        try {
            agent.getWorkerJvmManager().terminateWorkers();
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to terminate workers", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/stopTest")
    public String stopTest() throws Exception {
        try {
            //todo: timeout should be passed
            WorkerJvmManager workerJvmManager = agent.getWorkerJvmManager();
            workerJvmManager.executeOnWorkers(new StopTask(30000), "Stopping test");
            workerJvmManager.terminateWorkers();
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to stop test", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("/genericTestTask")
    public Object genericTestTask(String methodName) throws Exception {
        try {
            GenericTestTask task = new GenericTestTask(methodName);
            WorkerJvmManager workerJvmManager = agent.getWorkerJvmManager();
            workerJvmManager.executeOnWorkers(task, "Test " + methodName);
            return "OK";
        } catch (Exception e) {
            log.severe(format("Failed to execute test.%s()", methodName), e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("/echo")
    public String echo(String msg) throws Exception {
        try {
            agent.echo(msg);
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to echo", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/prepareForTest")
    public String prepareForTest(TestRecipe testRecipe) throws Exception {
        try {
            agent.setTestRecipe(testRecipe);
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to prepareForTest", e);
            throw e;
        }
    }
}