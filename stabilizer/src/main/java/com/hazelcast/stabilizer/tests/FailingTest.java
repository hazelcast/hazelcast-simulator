package com.hazelcast.stabilizer.tests;

import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.worker.ExceptionReporter;

import java.util.LinkedList;
import java.util.List;

//A test that causes a failure. This is useful fortesting the stabilizer framework and for demonstration purposes.
public class FailingTest {

    public String failure = "Exception";

    @Run
    public void run() {
        if (failure.equals("Exception")) {
            ExceptionReporter.report(new RuntimeException("Wanted exception"));
        } else if (failure.equals("OOME")) {
            List<byte[]> list = new LinkedList<byte[]>();
            for (; ; ) {
                try {
                    list.add(new byte[100 * 1000 * 1000]);
                } catch (OutOfMemoryError error) {

                }
            }
        } else if (failure.equals("Exit")) {
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        FailingTest test = new FailingTest();
        new TestRunner().run(test, 10);
    }
}
