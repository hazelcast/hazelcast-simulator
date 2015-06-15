package com.hazelcast.simulator.tests;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ReflectionUtils;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;

public class GenericOperationTest {

    private static final ILogger LOGGER = Logger.getLogger(GenericOperationTest.class);

    public enum PrioritySelector {
        PRIORITY,
        NORMAL
    }

    // properties
    public double priorityProb = 0.1;
    public int delayNs = 100 * 1000;
    // probes
    public IntervalProbe normalLatency;
    public IntervalProbe priorityLatency;
    public SimpleProbe throughput;


    private OperationService operationService;
    private Address[] memberAddresses;
    private final OperationSelectorBuilder<PrioritySelector> operationSelectorBuilder
            = new OperationSelectorBuilder<PrioritySelector>();
    private TestContext testContext;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        this.testContext = testContext;

        NodeEngineImpl nodeEngine = getNode(testContext.getTargetInstance()).nodeEngine;
        Method method = ReflectionUtils.getMethodByName(NodeEngineImpl.class, "getOperationService");
        this.operationService = (OperationService) method.invoke(nodeEngine);
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
            GenericOperation operation = new GenericOperation(delayNs);
            normalLatency.started();
            InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, address);
            f.getSafely();
            normalLatency.done();
        }

        private void invokePriorityOperation(Address address) {
            GenericPriorityOperation operation = new GenericPriorityOperation(delayNs);
            priorityLatency.started();
            InternalCompletableFuture f = operationService.invokeOnTarget(null, operation, address);
            f.getSafely();
            priorityLatency.done();
        }
    }

    public static class GenericOperation extends AbstractOperation {

        public int delayNanos;

        public GenericOperation(int delayNanos) {
            this();
            this.delayNanos = delayNanos;
        }

        public GenericOperation() {
            setPartitionId(-1);
        }

        @Override
        public void run() throws Exception {
            if (delayNanos > 0) {
                Random random = new Random();
                LockSupport.parkNanos(random.nextInt(delayNanos));
            }
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(delayNanos);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            delayNanos = in.readInt();
        }
    }

    public static class GenericPriorityOperation extends GenericOperation implements UrgentSystemOperation {

        public GenericPriorityOperation(int delayNanos) {
            super(delayNanos);
        }

        public GenericPriorityOperation() {
        }
    }

    public static void main(String[] args) throws Exception {
        GenericOperationTest test = new GenericOperationTest();
        new TestRunner<GenericOperationTest>(test).withDuration(10).run();
    }
}
