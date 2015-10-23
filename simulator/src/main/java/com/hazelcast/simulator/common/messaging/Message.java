/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.common.KeyValuePair;

import java.io.Serializable;
import java.util.Set;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public abstract class Message implements Serializable {

    private MessageAddress messageAddress;

    public Message(MessageAddress messageAddress) {
        if (messageAddress == null) {
            throw new IllegalArgumentException("Message address cannot be null");
        }
        this.messageAddress = messageAddress;
    }

    public MessageAddress getMessageAddress() {
        return messageAddress;
    }

    public boolean disableMemberFailureDetection() {
        return false;
    }

    public boolean removeFromAgentList() {
        return false;
    }

    public static String getMessageHelp() {
        Set<String> messageSpecs = MessagesFactory.getMessageSpecs();
        StringBuilder builder = new StringBuilder();
        for (String spec : messageSpecs) {
            builder.append("* ")
                    .append(spec)
                    .append(" - ")
                    .append(MessagesFactory.getMessageDescription(spec))
                    .append(NEW_LINE);
        }
        return builder.toString();
    }

    public static Message newBySpec(String messageTypeSpec, String messageAddressSpec) {
        return MessagesFactory.bySpec(messageTypeSpec, messageAddressSpec);
    }

    public static Message newBySpec(String messageTypeSpec, MessageAddress messageAddress) {
        return MessagesFactory.bySpec(messageTypeSpec, messageAddress);
    }

    public static Message newBySpecWithAttribute(String messageTypeSpec, String messageAddressSpec,
                                                 KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        return MessagesFactory.bySpec(messageTypeSpec, messageAddressSpec, attribute);
    }

    public static Message newBySpecWithAttribute(String messageTypeSpec, MessageAddress messageAddress,
                                                 KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        return MessagesFactory.bySpec(messageTypeSpec, messageAddress, attribute);
    }
}
