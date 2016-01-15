package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManager;
import static com.hazelcast.simulator.agent.HarakiriMonitorCli.createHarakiriMonitor;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HarakiriMonitorTest {

    private static final int DEFAULT_TEST_TIMEOUT = 5000;
    private static final String CLOUD_PROVIDER = PROVIDER_EC2;
    private static final String CLOUD_IDENTITY = "someIdentity";
    private static final String CLOUD_CREDENTIALS = "someCredentials";
    private static final int WAIT_SECONDS = 1;

    private final List<String> args = new ArrayList<String>();

    private HarakiriMonitor harakiriMonitor;

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManager();
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
    }

    @Test
    public void testCreateHarakiriMonitor() {
        addCloudProviderArgs();
        addCloudIdentityArgs();
        addCloudCredentialArgs();

        HarakiriMonitor harakiriMonitor = createHarakiriMonitor(getArgs());
        assertNotNull(harakiriMonitor);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHarakiriMonitor_noCloudProvider() {
        addCloudIdentityArgs();
        addCloudCredentialArgs();

        createHarakiriMonitor(getArgs());
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHarakiriMonitor_noCloudIdentity() {
        addCloudProviderArgs();
        addCloudCredentialArgs();

        createHarakiriMonitor(getArgs());
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHarakiriMonitor_noCloudCredential() {
        addCloudProviderArgs();
        addCloudIdentityArgs();

        createHarakiriMonitor(getArgs());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHarakiriMonitor_noEC2() {
        harakiriMonitor = new HarakiriMonitor("static", CLOUD_IDENTITY, CLOUD_PROVIDER, WAIT_SECONDS);
        harakiriMonitor.start();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHarakiriMonitor_isEC2() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                harakiriMonitor = new HarakiriMonitor(PROVIDER_EC2, "exit 0", WAIT_SECONDS);
                harakiriMonitor.start();
                countDownLatch.countDown();
            }
        };
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS * 2));

        assertEquals(0, countDownLatch.getCount());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHarakiriMonitor_isEC2_withFailure() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                harakiriMonitor = new HarakiriMonitor(PROVIDER_EC2, "exit 1", WAIT_SECONDS);
                harakiriMonitor.start();
                countDownLatch.countDown();
            }
        };
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS * 2));

        assertEquals(1, countDownLatch.getCount());
    }

    @Test
    public void testMain() throws InterruptedException {
        addCloudProviderArgs();
        addCloudIdentityArgs();
        addCloudCredentialArgs();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                HarakiriMonitor.main(getArgs());
                countDownLatch.countDown();
            }
        };
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(1));

        assertEquals(1, countDownLatch.getCount());
    }

    @Test(expected = ExitStatusOneException.class)
    public void testMain_withException() {
        HarakiriMonitor.main(getArgs());
    }

    private void addCloudProviderArgs() {
        args.add("--cloudProvider");
        args.add(CLOUD_PROVIDER);
    }

    private void addCloudIdentityArgs() {
        args.add("--cloudIdentity");
        args.add(CLOUD_IDENTITY);
    }

    private void addCloudCredentialArgs() {
        args.add("--cloudCredential");
        args.add(CLOUD_CREDENTIALS);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
