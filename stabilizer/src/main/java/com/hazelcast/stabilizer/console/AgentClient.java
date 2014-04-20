package com.hazelcast.stabilizer.console;

import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.agent.WorkerVmSettings;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static java.lang.String.format;
import static javax.ws.rs.client.Entity.entity;

//https://jersey.java.net/nonav/documentation/latest/user-guide.html

/**
 * List<StoreOrder> orders = client.target("http://example.com/webapi/read")
 * .path("allOrders")
 * .request(MediaType.APPLICATION_XML)
 * .get(new GenericType<List<StoreOrder>>() {});
 */
public class AgentClient {

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

    public void spawnWorkers(WorkerVmSettings settings) {
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
                .get(new GenericType<List<Failure>>() {});
        //System.out.println(response);
         return response;
    }
}
