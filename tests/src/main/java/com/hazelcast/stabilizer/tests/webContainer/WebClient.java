package com.hazelcast.stabilizer.tests.webContainer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;


import com.hazelcast.stabilizer.worker.OperationSelector;
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
import java.util.Random;

import static org.junit.Assert.assertEquals;


/*
*
* A Load producing WebClient test.  making http Put and Get requests,  the the serverIp address / port configured in the
* properties.  each thread of this test represents a http client / "browser".
*
* */
public class WebClient {

    private final static ILogger log = Logger.getLogger(WebClient.class);

    public String serverIp="";
    public int serverPort=0;

    public String id;
    public int threadCount = 3;
    public int maxKeys=1000;
    public double getRequestProb = 0.5;
    public double postRequestProb = 0.5;

    private TestContext testContext;

    private enum RequestType {
        GET_REQUEST,
        PUT_REQUEST
    }

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        id = testContext.getTestId();

    }

    @Warmup(global = true)
    public void warmup() throws Exception { }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private CookieStore cookieStore;
        private HttpClient client;
        private String baseRul = "http://"+serverIp+":"+serverPort+"/";

        private Random random = new Random();
        private OperationSelector<RequestType> requestSelector = new OperationSelector<RequestType>();

        HashMap<Integer, String> putKeyValues = new HashMap();

        public Worker(){
            cookieStore = new BasicCookieStore();
            client = HttpClientBuilder.create().disableRedirectHandling().setDefaultCookieStore(cookieStore).build();

            log.info(id+": baseRul="+baseRul + " cookie="+cookieStore);

            requestSelector.addOperation(RequestType.GET_REQUEST, getRequestProb)
                           .addOperation(RequestType.PUT_REQUEST, postRequestProb);


            try{
                for(int key=0; key<maxKeys; key++){
                    int val = random.nextInt();
                    String res = putRequest("key/"+key+"/"+val);
                    putKeyValues.put(key, res);
                }
            }catch(Exception e){throw new RuntimeException(e);}
        }

        public void run() {
            while (!testContext.isStopped()) {
                try {
                    int key = random.nextInt(maxKeys);
                    String res;
                    switch ( requestSelector.select() ) {
                        case PUT_REQUEST:
                            int val = random.nextInt();
                            res = putRequest("key/"+key+"/"+val);
                            putKeyValues.put(key, res);

                            log.info(id+": put key="+key+" val="+res);
                            break;

                        case GET_REQUEST:
                            res = getRequest("key/"+key);

                            assertEquals(id+": not what i put", res, putKeyValues.get(key) );

                            log.info(id + ": get key=" + key + " val=" + res);
                            break;
                    }
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
            }
        }

        protected String getRequest(String restOpp) throws IOException {
            HttpUriRequest request = new HttpGet(baseRul+restOpp);
            return responseToString( client.execute(request) );
        }

        protected String putRequest(String restOpp)  throws IOException{
            HttpUriRequest request = new HttpPut(baseRul+restOpp);
            return responseToString( client.execute(request) );
        }

        protected String responseToString(HttpResponse response) throws IOException{
            HttpEntity entity = response.getEntity();
            return EntityUtils.toString(entity);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception { }
}