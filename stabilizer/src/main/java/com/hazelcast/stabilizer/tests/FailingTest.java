package com.hazelcast.stabilizer.tests;

import com.hazelcast.stabilizer.worker.ExceptionReporter;

public class FailingTest extends AbstractTest {
    @Override
    public void localSetup() throws Exception {
        spawn(new Worker());
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            ExceptionReporter.report(new RuntimeException("Wanted exception"));
        }
    }

    public static void main(String[] args) throws Exception {
        FailingTest test = new FailingTest();
        new TestRunner().run(test, 10);
    }
}
