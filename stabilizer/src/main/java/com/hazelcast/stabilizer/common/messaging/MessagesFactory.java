package com.hazelcast.stabilizer.common.messaging;

import org.apache.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class MessagesFactory {
    private final static Logger log = Logger.getLogger(MessagesFactory.class);
    private static final MessagesFactory instance = new MessagesFactory();

    private final Map<String, Constructor<? extends Message>> specs;
    private final MessageAddressParser messageAddressParser;

    private MessagesFactory() {
        specs = findMessages();
        messageAddressParser = new MessageAddressParser();
    }

    static Message bySpec(String messageTypeSpec, String messageAddressSpec) {
        Constructor<? extends Message> constructor = instance.specs.get(messageTypeSpec);
        if (constructor == null) {
            throw new IllegalArgumentException("Unknown message type " + messageTypeSpec + ".");
        }
        MessageAddress messageAddress = instance.messageAddressParser.parse(messageAddressSpec);
        try {
            return constructor.newInstance(messageAddress);
        } catch (InstantiationException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        }
    }

    private Map<String, Constructor<? extends Message>> findMessages() {
        Reflections reflections = new Reflections("");
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(MessageSpec.class);

        Map<String, Constructor<? extends Message>> messageSpecs = new HashMap<String, Constructor<? extends Message>>();
        for (Class<?> clazz : typesAnnotatedWith) {
            if (Message.class.isAssignableFrom(clazz)) {
                Class<? extends Message> messageClass = (Class<? extends Message>) clazz;
                try {
                    Constructor<? extends Message> constructor = messageClass.getConstructor(MessageAddress.class);
                    MessageSpec messageSpec = messageClass.getAnnotation(MessageSpec.class);
                    String spec = messageSpec.value();
                    messageSpecs.put(spec, constructor);
                } catch (NoSuchMethodException e) {
                    log.error("Error while searching for message types. Does the "
                            +clazz.getName()+" have a constructor with "+MessageAddress.class.getName()+" as an argument?", e);
                }
            } else {
                log.warn("Class "+clazz.getName()+" is annotated with "+MessageSpec.class.getName()+", however it does" +
                        "not extend "+Message.class.getName()+".");
            }
        }
        return messageSpecs;
    }
}
