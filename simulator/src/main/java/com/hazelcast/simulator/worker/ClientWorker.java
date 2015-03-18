package com.hazelcast.simulator.worker;

/**
 * This class serves no other purpose than to provide a name in the jps listing that reflects that the JVM is a client JVM.
 * It has no other purpose and it will delegate all its work to the {@link MemberWorker} class.
 */
public class ClientWorker {

    public static void main(String[] args) {
        MemberWorker.main(args);
    }
}
