package com.hazelcast.stabilizer.agent;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;

//todo: class needs to be merged in agent
public class AgentRestServer {

    //todo: we don't want to bind on localhost
    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://localhost:8080/agent/";

    private HttpServer server;

    public void start() {
        ResourceConfig rc = new ResourceConfig().packages("com.hazelcast.stabilizer.agent");
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }
}
