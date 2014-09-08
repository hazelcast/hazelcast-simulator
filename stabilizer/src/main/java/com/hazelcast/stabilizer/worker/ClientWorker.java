package com.hazelcast.stabilizer.worker;


/**
 * This class serves no other purpose that to provide a name in the jps listing that reflects that the jvm is a client
 * jvm. It has no other purpose and it will delegate all its work to the {@link MemberWorker}
 * class.
 */
public class ClientWorker {

    public static void main(String[] args) {
        MemberWorker.main(args);
    }
}
