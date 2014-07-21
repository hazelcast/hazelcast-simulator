package com.hazelcast.stabilizer.common.messaging;

/**
 * A NewMemberMessage is send to indicate to an agent that he should create a new member in the cluster.
 */
@MessageSpec("newMember")
public class NewMemberMessage extends Message{

    public NewMemberMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public String toString(){
        return "NewMemberMessage";
    }
}
