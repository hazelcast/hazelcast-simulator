package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.utils.AssertTask;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertNotNull;

public class StubPromise implements Promise {

    private volatile Object answer;

    @Override
    public void answer(Object value) {
        this.answer = value;
    }

    public boolean hasAnswer() {
        return answer != null;
    }

    public Object getAnswer() {
        return answer;
    }

    public void assertCompletesEventually() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(answer);
            }
        });
    }
}
