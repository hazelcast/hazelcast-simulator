package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.commands.CommandRequest;
import com.hazelcast.simulator.worker.commands.CommandResponse;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

/**
 * Reads {@link com.hazelcast.simulator.worker.commands.CommandRequest} instances from the network and returns {@link com.hazelcast.simulator.worker.commands.CommandResponse} instances for {@link MemberWorker}
 * and {@link ClientWorker} instances.
 */
class WorkerSocketProcessor {

    private final BlockingQueue<CommandRequest> requestQueue;
    private final BlockingQueue<CommandResponse> responseQueue;

    private final String workerId;
    private final WorkerSocketThread workerSocketThread;

    public WorkerSocketProcessor(BlockingQueue<CommandRequest> requestQueue, BlockingQueue<CommandResponse> responseQueue,
                                 String workerId) {
        this.requestQueue = requestQueue;
        this.responseQueue = responseQueue;
        this.workerId = workerId;

        workerSocketThread = new WorkerSocketThread();
        workerSocketThread.start();
    }

    void shutdown() {
        workerSocketThread.running = false;
    }

    private class WorkerSocketThread extends Thread {
        private volatile boolean running = true;

        private WorkerSocketThread() {
            super("WorkerSocketThread");
        }

        @Override
        public void run() {
            while (running) {
                try {
                    List<CommandRequest> requests = execute(WorkerJvmManager.SERVICE_POLL_WORK, workerId);
                    for (CommandRequest request : requests) {
                        requestQueue.add(request);
                    }

                    CommandResponse response = responseQueue.poll(1, TimeUnit.SECONDS);
                    if (response == null) {
                        continue;
                    }

                    sendResponse(singletonList(response));

                    List<CommandResponse> responses = new LinkedList<CommandResponse>();
                    responseQueue.drainTo(responses);

                    sendResponse(responses);
                } catch (Throwable e) {
                    ExceptionReporter.report(null, e);
                }
            }
        }
    }

    private void sendResponse(List<CommandResponse> responses) throws Exception {
        for (CommandResponse response : responses) {
            execute(WorkerJvmManager.COMMAND_PUSH_RESPONSE, workerId, response);
        }
    }

    // create a new socket for every request to avoid dependency on the state of a socket so nasty stuff can be done
    @SuppressWarnings("unchecked")
    private <E> E execute(String service, Object... args) throws Exception {
        Socket socket = new Socket(InetAddress.getByName(null), WorkerJvmManager.PORT);
        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(service);
            for (Object arg : args) {
                oos.writeObject(arg);
            }
            oos.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Object response = in.readObject();

            if (response instanceof TerminateWorkerException) {
                System.exit(0);
            }

            if (response instanceof Exception) {
                Exception exception = (Exception) response;
                CommonUtils.fixRemoteStackTrace(exception, Thread.currentThread().getStackTrace());
                throw exception;
            }

            return (E) response;
        } finally {
            CommonUtils.closeQuietly(socket);
        }
    }
}
