/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.protocol.operation.ChaosMonkeyOperation;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static com.hazelcast.simulator.utils.NativeUtils.kill;
import static java.lang.String.format;

public final class ChaosMonkeyUtils {

    static final int DEFAULT_SPIN_THREAD_COUNT = 1;
    static final int DEFAULT_ALLOCATION_INTERVAL_MILLIS = 0;

    private static final String BLOCK_TRAFFIC_PORTS = "5700:5800";
    private static final int BUFFER_SIZE = 1000;

    private static final AtomicInteger SPIN_THREAD_COUNT = new AtomicInteger(DEFAULT_SPIN_THREAD_COUNT);
    private static final AtomicInteger ALLOCATION_INTERVAL_MILLIS = new AtomicInteger(DEFAULT_ALLOCATION_INTERVAL_MILLIS);
    private static final Queue<Thread> THREADS = new LinkedBlockingQueue<Thread>();

    private static final List<Object> ALLOCATION_LIST = new ArrayList<Object>();
    private static final Logger LOGGER = Logger.getLogger(ChaosMonkeyUtils.class);

    private ChaosMonkeyUtils() {
    }

    public static void execute(ChaosMonkeyOperation.Type type) {
        switch (type) {
            case BLOCK_TRAFFIC:
                String command = format("sudo /sbin/iptables -p tcp --dport %s -A INPUT -i eth0 -j REJECT", BLOCK_TRAFFIC_PORTS);
                NativeUtils.execute(command);
                break;
            case UNBLOCK_TRAFFIC:
                command = "sudo /sbin/iptables -F";
                NativeUtils.execute(command);
                break;
            case SPIN_CORE_INDEFINITELY:
                int spinThreadCount = SPIN_THREAD_COUNT.get();
                for (int i = 0; i < spinThreadCount; i++) {
                    Thread thread = new BusySpinner();
                    THREADS.add(thread);
                    thread.start();
                }
                break;
            case USE_ALL_MEMORY:
                Thread thread = new MemoryConsumer(ALLOCATION_INTERVAL_MILLIS.get());
                THREADS.add(thread);
                thread.start();
                break;
            case SOFT_KILL:
                LOGGER.warn("Processing soft kill message. I'm about to die!");
                exitWithError();
                break;
            case HARD_KILL:
                int pid = getPID();
                LOGGER.info("Sending kill -9 signal to own PID: " + pid);
                if (pid > 0) {
                    kill(pid);
                }
                break;
            default:
                LOGGER.info("This is a NOOP for integration tests");
        }
    }

    static void interruptThreads() {
        for (Thread thread : THREADS) {
            thread.interrupt();
            joinThread(thread);
        }
    }

    static void setSpinThreadCount(int spinThreadCount) {
        SPIN_THREAD_COUNT.set(spinThreadCount);
    }

    static void setAllocationIntervalMillis(int allocationIntervalMillis) {
        ALLOCATION_INTERVAL_MILLIS.set(allocationIntervalMillis);
    }

    private static final class BusySpinner extends Thread {

        private final Random random = new Random();

        @Override
        public void run() {
            try {
                while (!interrupted()) {
                    if (random.nextInt(BUFFER_SIZE) == BUFFER_SIZE + BUFFER_SIZE) {
                        LOGGER.fatal("Can't happen!");
                    }
                }
            } finally {
                THREADS.remove(this);
            }
        }
    }

    private static final class MemoryConsumer extends Thread {

        private final int allocationIntervalMillis;

        MemoryConsumer(int allocationIntervalMillis) {
            this.allocationIntervalMillis = allocationIntervalMillis;
        }

        @Override
        public void run() {
            LOGGER.info("Starting a thread to consume all memory");
            while (!interrupted()) {
                try {
                    allocateMemory();
                } catch (OutOfMemoryError e) {
                    EmptyStatement.ignore(e);
                }
            }
        }

        private void allocateMemory() {
            while (!isInterrupted()) {
                byte[] buff = new byte[BUFFER_SIZE];
                ALLOCATION_LIST.add(buff);
                sleepMillisInterruptThread(allocationIntervalMillis);
            }
        }

        private void sleepMillisInterruptThread(int sleepMillis) {
            try {
                TimeUnit.MILLISECONDS.sleep(sleepMillis);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted during sleep on map size: " + ALLOCATION_LIST.size());
                Thread.currentThread().interrupt();
            }
        }
    }
}
