package com.hazelcast.stabilizer.agent;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.worker.tasks.GenericTestTask;
import com.hazelcast.stabilizer.worker.tasks.InitTest;
import com.hazelcast.stabilizer.worker.tasks.StopTask;
import com.hazelcast.stabilizer.worker.WorkerVmSettings;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.Callable;

import static java.lang.String.format;

@Path("agent")
public class AgentService {
    private final static ILogger log = Logger.getLogger(AgentService.class);

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/getIt")
    public String getIt() {
        return "Got it!";
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/spawnWorkers")
    public String spawnWorkers(WorkerVmSettings settings) throws Exception {
        try {
            Agent agent = Agent.agent;
            agent.getWorkerVmManager().spawn(settings);
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/execute")
    public String execute(Callable command) throws Exception {
        try {
            System.out.println("Processing callable:" + command);
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/initTest")
    public String initTest(TestRecipe testRecipe) throws Exception {
        try {
            log.info("Init Test:\n" + testRecipe);

            Agent agent = Agent.agent;

            InitTest initTest = new InitTest(testRecipe);
            agent.shoutToWorkers(initTest, "Test Initializing");
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to init Test", e);
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/cleanWorkersHome")
    public String cleanWorkersHome() throws Exception {
        try {
            Agent agent = Agent.agent;
            agent.cleanWorkersHome();
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/terminateWorkers")
    public String terminateWorkers() throws Exception {
        try {
            Agent agent = Agent.agent;
            agent.getWorkerVmManager().terminateWorkers();
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/stopTest")
    public String stopTest() throws Exception {
        try {
            Agent agent = Agent.agent;
            //todo: timeout should be passed
            agent.shoutToWorkers(new StopTask(30000), "Stopping test");
            agent.getWorkerVmManager().terminateWorkers();
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.TEXT_PLAIN)
    @Path("/genericTestTask")
    public Object genericTestTask(String methodName) throws Exception {
        try {
            Agent agent = Agent.agent;
            GenericTestTask task = new GenericTestTask(methodName);
            agent.shoutToWorkers(task, "Test " + methodName);
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
            Agent agent = Agent.agent;
            agent.echo(msg);
            return "OK";
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PUT
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/prepareForTest")
    public String prepareForTest(TestRecipe testRecipe) throws Exception {
        try {
            log.info("Init Test:\n" + testRecipe);
            Agent agent = Agent.agent;
            agent.setTestRecipe(testRecipe);
            return "OK";
        } catch (Exception e) {
            log.severe("Failed to init Test", e);
            throw e;
        }
    }
}