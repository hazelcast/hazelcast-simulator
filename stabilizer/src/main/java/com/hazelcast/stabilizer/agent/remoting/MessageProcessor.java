package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeoutException;

public class MessageProcessor {
    private static final Logger log = Logger.getLogger(MessageProcessor.class);

    private WorkerJvmManager workerJvmManager;

    public MessageProcessor(WorkerJvmManager workerJvmManager) {
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
        } else {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    private void processLocalRunnableMessage(Runnable message) {
        message.run();
    }
}
