package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.common.messaging.TerminateRandomWorker;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeoutException;

public class AgentMessageProcessor {
    private static final Logger log = Logger.getLogger(AgentMessageProcessor.class);

    private WorkerJvmManager workerJvmManager;

    public AgentMessageProcessor(WorkerJvmManager workerJvmManager) {
        this.workerJvmManager = workerJvmManager;
    }

    public void process(Message message) {
        MessageAddress messageAddress = message.getMessageAddress();
        if (messageAddress.getWorkerAddress() == null) {
            processLocalMessage(message);
        } else {
            processWorkerMessage(message);
        }
    }

    private void processWorkerMessage(Message message) {
        try {
            workerJvmManager.sendMessage(message);
        } catch (TimeoutException e) {
            log.error(e);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    private void processLocalMessage(Message message) {
        log.debug("Processing local message :"+message);
        if (message instanceof Runnable) {
            processLocalRunnableMessage((Runnable) message);
        } else if (message instanceof TerminateRandomWorker) {
            workerJvmManager.terminateRandomWorker();
        } else {
            throw new UnsupportedOperationException("Unknown message type received: "+message.getClass().getName());
        }
    }

    private void processLocalRunnableMessage(Runnable message) {
        message.run();
    }
}
