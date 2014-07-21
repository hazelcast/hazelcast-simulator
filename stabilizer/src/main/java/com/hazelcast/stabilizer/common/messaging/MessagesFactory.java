package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.common.KeyValuePair;
import org.apache.log4j.Logger;
import org.reflections.Reflections;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.Key;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class MessagesFactory {
    private final static Logger log = Logger.getLogger(MessagesFactory.class);
    private static final MessagesFactory instance = new MessagesFactory();

    private final Map<String, Constructor<? extends Message>> noAttributeConstructors;
    private final Map<String, Constructor<? extends Message>> attributeConstructors;
    private final MessageAddressParser messageAddressParser;

    private MessagesFactory() {
        noAttributeConstructors = new HashMap<String, Constructor<? extends Message>>();
        attributeConstructors = new HashMap<String, Constructor<? extends Message>>();
        messageAddressParser = new MessageAddressParser();
        findAndInitMessageTypes();
    }

    static Set<String> getMessageSpecs() {
        HashSet<String> constructors = new HashSet<String>(instance.noAttributeConstructors.keySet());
        constructors.addAll(instance.attributeConstructors.keySet());
        return constructors;
    }

    static Message bySpec(String messageTypeSpec, String messageAddressSpec) {
        MessageAddress address = instance.messageAddressParser.parse(messageAddressSpec);
        return bySpec(messageTypeSpec, address);
    }

    static Message bySpec(String messageTypeSpec, MessageAddress messageAddress) {
        Constructor<? extends Message> constructor = instance.noAttributeConstructors.get(messageTypeSpec);

        if (constructor == null) {
            throw new IllegalArgumentException("Unknown message type " + messageTypeSpec + ".");
        }
        return createInstance(constructor, messageAddress, null);
    }

    static Message bySpec(String messageTypeSpec, String messageAddressSpec,
                          KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        MessageAddress address = instance.messageAddressParser.parse(messageAddressSpec);
        return bySpec(messageTypeSpec, address, attribute);
    }

    static Message bySpec(String messageTypeSpec, MessageAddress messageAddress,
                          KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        Constructor<? extends Message> constructor = instance.attributeConstructors.get(messageTypeSpec);

        if (constructor == null) {
            throw new IllegalArgumentException("Unknown message type " + messageTypeSpec + ".");
        }
        return createInstance(constructor, messageAddress, attribute);
    }

    private static Message createInstance(Constructor<? extends Message> constructor, MessageAddress messageAddress,
                                          KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        try {
            return attribute == null ? constructor.newInstance(messageAddress) : constructor.newInstance(messageAddress, attribute);
        } catch (InstantiationException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        }
    }

    private void findAndInitMessageTypes() {
        Reflections reflections = new Reflections("");
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(MessageSpec.class);

        for (Class<?> clazz : typesAnnotatedWith) {
            if (Message.class.isAssignableFrom(clazz)) {
                registerMessage((Class<? extends Message>) clazz);
            } else {
                log.warn("Class "+clazz.getName()+" is annotated with "+MessageSpec.class.getName()+", however it does" +
                        "not extend "+Message.class.getName()+".");
            }
        }
    }

    private void registerMessage(Class<? extends Message> clazz) {
        String spec = getSpecString(clazz);
        try {
            Constructor<? extends Message> constructor = clazz.getConstructor(MessageAddress.class);
            noAttributeConstructors.put(spec, constructor);
        } catch (NoSuchMethodException e) {
            log.error("Error while searching for message types. Does the "
                    +clazz.getName()+" have a constructor with "+MessageAddress.class.getName()+" as an argument?", e);
        }
        try {
            Constructor<? extends Message> constructor = clazz.getConstructor(MessageAddress.class, KeyValuePair.class);
            attributeConstructors.put(spec, constructor);
        } catch (NoSuchMethodException e) {
            log.debug("Class "+clazz.getName()+" does not have a constructor accepting "+KeyValuePair.class.getName()+"+.");
        }

    }

    private String getSpecString(Class<? extends Message> messageClass) {
        MessageSpec messageSpec = messageClass.getAnnotation(MessageSpec.class);
        return messageSpec.value();
    }
}
