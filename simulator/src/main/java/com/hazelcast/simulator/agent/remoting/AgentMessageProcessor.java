package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.common.messaging.NewMemberMessage;
import com.hazelcast.simulator.common.messaging.TerminateRandomWorkerMessage;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.common.messaging.Message;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class AgentMessageProcessor {
    private static final Logger log = Logger.getLogger(AgentMessageProcessor.class);
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private WorkerJvmManager workerJvmManager;

    public AgentMessageProcessor(WorkerJvmManager workerJvmManager) {
        this.workerJvmManager = workerJvmManager;
    }

    public void submit(final Message message) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    MessageAddress messageAddress = message.getMessageAddress();
                    if (shouldProcess(message)) {
                        if (messageAddress.getWorkerAddress() == null) {
                            processLocalMessage(message);
                        } else {
                            processWorkerMessage(message);
                        }
                    }
                } catch (Throwable t) {
                    log.error("Failed to process message:" + message, t);
                }
            }
        });
    }

    private boolean shouldProcess(Message message) {
        return true;
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

    private void processLocalMessage(Message message) throws Exception {
        log.debug("Processing local message :" + message);
        if (message instanceof Runnable) {
            processLocalRunnableMessage((Runnable) message);
        } else if (message instanceof TerminateRandomWorkerMessage) {
            workerJvmManager.terminateRandomWorker();
        } else if (message instanceof NewMemberMessage) {
            workerJvmManager.newMember();
        } else {
            throw new UnsupportedOperationException("Unknown message type received: " + message.getClass().getName());
        }
    }

    private void processLocalRunnableMessage(Runnable message) {
        message.run();
    }
}
