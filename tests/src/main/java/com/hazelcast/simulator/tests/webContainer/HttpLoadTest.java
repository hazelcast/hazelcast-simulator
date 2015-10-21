package com.hazelcast.simulator.tests.webContainer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * A load producing HTTP test. Making HTTP PUT and GET requests. The ip addresses and ports of the members are configured in the
 * properties. Each thread of this test represents a HTTP client.
 */
public class HttpLoadTest {

    private static final ILogger LOGGER = Logger.getLogger(HttpLoadTest.class);

    private enum RequestType {
        GET_REQUEST,
        PUT_REQUEST
    }

    public String serverIp = "";
    public int serverPort = 0;

    public String id;
    public int threadCount = 3;
    public int maxKeys = 1000;
    public double getRequestProb = 0.5;
    public double postRequestProb = 0.5;

    private final OperationSelectorBuilder<RequestType> operationSelectorBuilder = new OperationSelectorBuilder<RequestType>();

    private TestContext testContext;
    private String baseRul;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        id = testContext.getTestId();

        baseRul = "http://" + serverIp + ':' + serverPort + '/';

        operationSelectorBuilder
                .addOperation(RequestType.GET_REQUEST, getRequestProb)
                .addOperation(RequestType.PUT_REQUEST, postRequestProb);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {

        private final Random random = new Random();
        private final Map<Integer, String> putKeyValues = new HashMap<Integer, String>();
        private final CookieStore cookieStore = new BasicCookieStore();

        private final OperationSelector<RequestType> requestSelector = operationSelectorBuilder.build();

        private final HttpClient client;

        public Worker() {
            LOGGER.info(id + ": baseRul=" + baseRul + " cookie=" + cookieStore);

            client = HttpClientBuilder.create().disableRedirectHandling().setDefaultCookieStore(cookieStore).build();

            try {
                for (int key = 0; key < maxKeys; key++) {
                    int val = random.nextInt();
                    String res = putRequest("key/" + key + '/' + val);
                    putKeyValues.put(key, res);
                }
            } catch (Exception e) {
                throw new TestException(e);
            }
        }

        public void run() {
            while (!testContext.isStopped()) {
                try {
                    int key = random.nextInt(maxKeys);
                    String res;
                    switch (requestSelector.select()) {
                        case PUT_REQUEST:
                            int val = random.nextInt();
                            res = putRequest("key/" + key + '/' + val);
                            putKeyValues.put(key, res);
                            break;

                        case GET_REQUEST:
                            res = getRequest("key/" + key);
                            assertEquals(id + ": not what i put", res, putKeyValues.get(key));
                            break;

                        default:
                            throw new UnsupportedOperationException();
                    }
                } catch (IOException e) {
                    throw new TestException(e);
                }
            }
        }

        private String getRequest(String restOpp) throws IOException {
            HttpUriRequest request = new HttpGet(baseRul + restOpp);
            return responseToString(client.execute(request));
        }

        private String putRequest(String restOpp) throws IOException {
            HttpUriRequest request = new HttpPut(baseRul + restOpp);
            return responseToString(client.execute(request));
        }

        private String responseToString(HttpResponse response) throws IOException {
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity);
        }
    }
}
