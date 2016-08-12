package com.hazelcast.simulator.harakiri;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManager;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.assertEquals;

public class HarakiriMonitorCliTest {
    private static final String CLOUD_PROVIDER = PROVIDER_EC2;
    private static final String CLOUD_IDENTITY = "someIdentity";
    private static final String CLOUD_CREDENTIALS = "someCredentials";

    private final List<String> args = new ArrayList<String>();

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

        new HarakiriMonitorCli(getArgs());
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHarakiriMonitor_noCloudProvider() {
        addCloudIdentityArgs();
        addCloudCredentialArgs();

        new HarakiriMonitorCli(getArgs());
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHarakiriMonitor_noCloudIdentity() {
        addCloudProviderArgs();
        addCloudCredentialArgs();

        new HarakiriMonitorCli(getArgs());
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHarakiriMonitor_noCloudCredential() {
        addCloudProviderArgs();
        addCloudIdentityArgs();

        new HarakiriMonitorCli(getArgs());
    }

    @Test
    public void testMain() {
        addCloudProviderArgs();
        addCloudIdentityArgs();
        addCloudCredentialArgs();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @Override
            public void run() {
                HarakiriMonitorCli.main(getArgs());
                countDownLatch.countDown();
            }
        };
        thread.start();
        joinThread(thread, TimeUnit.SECONDS.toMillis(1));

        assertEquals(1, countDownLatch.getCount());
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
        return args.toArray(new String[0]);
    }
}
