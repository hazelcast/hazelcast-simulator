package com.hazelcast.simulator.tests;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;

public class GenericOperationTest {

    private static final ILogger LOGGER = Logger.getLogger(GenericOperationTest.class);

    private InternalOperationService operationService;
    private Address[] memberAddresses;

    private enum PrioritySelector {
        PRIORITY,
        NORMAL
    }

    // properties
    public double priorityProb = 0.1;

    // probes
    public IntervalProbe normalLatency;
    public IntervalProbe priorityLatency;
    public SimpleProbe throughput;

    private final OperationSelectorBuilder<PrioritySelector> operationSelectorBuilder = new OperationSelectorBuilder<PrioritySelector>();

    private TestContext testContext;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.operationService = getNode(testContext.getTargetInstance()).nodeEngine.getOperationService();
        operationSelectorBuilder
                .addOperation(PrioritySelector.PRIORITY, priorityProb)
                .addDefaultOperation(PrioritySelector.NORMAL);
    }

    @Warmup
    public void warmup() {
        Set<Member> memberSet = testContext.getTargetInstance().getCluster().getMembers();
        memberAddresses = new Address[memberSet.size()];

        int k = 0;
        for (Member member : memberSet) {
            memberAddresses[k] = new Address(member.getSocketAddress());
            k++;
        }
    }

    @Teardown
    public void tearDown() throws Exception {
        LOGGER.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<PrioritySelector> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(PrioritySelector operationSelector) {
            Address address = randomAddress();

            switch (operationSelector) {
                case PRIORITY:
                    invokePriorityOperation(address);
                    break;
                case NORMAL:
                    invokeNormalOperation(address);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            throughput.done();
        }

        private Address randomAddress() {
            return memberAddresses[randomInt(memberAddresses.length)];
        }

        private void invokeNormalOperation(Address address) {
            GenericOperation operation = new GenericOperation();
            normalLatency.started();
            InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, address);
            f.getSafely();
            normalLatency.done();
        }

        private void invokePriorityOperation(Address address) {
            GenericOperation operation = new GenericOperation();
            priorityLatency.started();
            InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, address);
            f.getSafely();
            priorityLatency.done();
        }
    }

    public static class GenericOperation extends AbstractOperation {

        public GenericOperation(){
            setPartitionId(-1);
        }

        @Override
        public void run() throws Exception {
            Random random = new Random();
            LockSupport.parkNanos(random.nextInt(200 * 1000));
        }
    }

    public static class GenericPriorityOperation extends GenericOperation implements UrgentSystemOperation {
    }

    public static void main(String[] args) throws Exception {
        GenericOperationTest test = new GenericOperationTest();
        new TestRunner<GenericOperationTest>(test).withDuration(10).run();
    }
}
