package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.common.KeyValuePair;
import org.apache.log4j.Logger;
import org.reflections.Reflections;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

final class MessagesFactory {
    private static final Logger LOGGER = Logger.getLogger(MessagesFactory.class);
    private static final MessagesFactory INSTANCE = new MessagesFactory();

    private final Map<String, Constructor<? extends Message>> noAttributeConstructors;
    private final Map<String, Constructor<? extends Message>> attributeConstructors;
    private final Map<String, String> messageDescription;
    private final MessageAddressParser messageAddressParser;

    private MessagesFactory() {
        noAttributeConstructors = new HashMap<String, Constructor<? extends Message>>();
        attributeConstructors = new HashMap<String, Constructor<? extends Message>>();
        messageAddressParser = new MessageAddressParser();
        messageDescription = new HashMap<String, String>();
        findAndInitMessageTypes();
    }

    static Set<String> getMessageSpecs() {
        HashSet<String> constructors = new HashSet<String>(INSTANCE.noAttributeConstructors.keySet());
        constructors.addAll(INSTANCE.attributeConstructors.keySet());
        return constructors;
    }

    static String getMessageDescription(String messageSpec) {
        String description = INSTANCE.messageDescription.get(messageSpec);
        if (description == null) {
            throw new IllegalArgumentException("Unknown message type '" + messageSpec + "'.");
        }
        return description;
    }

    static Message bySpec(String messageTypeSpec, String messageAddressSpec) {
        MessageAddress address = INSTANCE.messageAddressParser.parse(messageAddressSpec);
        return bySpec(messageTypeSpec, address);
    }

    static Message bySpec(String messageTypeSpec, MessageAddress messageAddress) {
        Constructor<? extends Message> constructor = INSTANCE.noAttributeConstructors.get(messageTypeSpec);

        if (constructor == null) {
            throw new IllegalArgumentException("Unknown message type " + messageTypeSpec + ".");
        }
        return createInstance(constructor, messageAddress, null);
    }

    static Message bySpec(String messageTypeSpec, String messageAddressSpec,
                          KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        MessageAddress address = INSTANCE.messageAddressParser.parse(messageAddressSpec);
        return bySpec(messageTypeSpec, address, attribute);
    }

    static Message bySpec(String messageTypeSpec, MessageAddress messageAddress,
                          KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        Constructor<? extends Message> constructor = INSTANCE.attributeConstructors.get(messageTypeSpec);

        if (constructor == null) {
            throw new IllegalArgumentException("Unknown message type " + messageTypeSpec + ".");
        }
        return createInstance(constructor, messageAddress, attribute);
    }

    private static Message createInstance(Constructor<? extends Message> constructor, MessageAddress messageAddress,
                                          KeyValuePair<? extends Serializable, ? extends Serializable> attribute) {
        try {
            if (attribute == null) {
                return constructor.newInstance(messageAddress);
            } else {
                return constructor.newInstance(messageAddress, attribute);
            }
        } catch (InstantiationException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Error while creating a new message", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void findAndInitMessageTypes() {
        Reflections reflections = new Reflections("");
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(MessageSpec.class);

        for (Class<?> clazz : typesAnnotatedWith) {
            if (Message.class.isAssignableFrom(clazz)) {
                registerMessage((Class<? extends Message>) clazz);
            } else {
                LOGGER.warn(format("Class %s is annotated with %s, however it does not extend %s", clazz.getName(),
                        MessageSpec.class.getName(), Message.class.getName()));
            }
        }
    }

    private void registerMessage(Class<? extends Message> clazz) {
        boolean registered = registerMessageTryBasicConstructor(clazz);
        if (registerMessageTryConstructorWithKeyValue(clazz) || registered) {
            registerDescription(clazz);
        }

    }

    private void registerDescription(Class<? extends Message> clazz) {
        String specString = getSpecString(clazz);
        String description = getDescription(clazz);
        messageDescription.put(specString, description);
    }

    private Boolean registerMessageTryConstructorWithKeyValue(Class<? extends Message> clazz) {
        String spec = getSpecString(clazz);
        try {
            Constructor<? extends Message> constructor = clazz.getConstructor(MessageAddress.class, KeyValuePair.class);
            attributeConstructors.put(spec, constructor);
            return true;
        } catch (NoSuchMethodException e) {
            LOGGER.debug(format("Class %s does not have a constructor accepting %s",
                    clazz.getName(), KeyValuePair.class.getName()));
            return false;
        }
    }

    private boolean registerMessageTryBasicConstructor(Class<? extends Message> clazz) {
        String spec = getSpecString(clazz);
        try {
            Constructor<? extends Message> constructor = clazz.getConstructor(MessageAddress.class);
            noAttributeConstructors.put(spec, constructor);
            return true;
        } catch (NoSuchMethodException e) {
            LOGGER.error(format("Error while searching for message types. Does the %s have a constructor with %s as an argument?",
                    clazz.getName(), MessageAddress.class.getName()), e);
            return false;
        }
    }

    private String getSpecString(Class<? extends Message> messageClass) {
        MessageSpec messageSpec = messageClass.getAnnotation(MessageSpec.class);
        return messageSpec.value();
    }

    private String getDescription(Class<? extends Message> messageClass) {
        MessageSpec messageSpec = messageClass.getAnnotation(MessageSpec.class);
        return messageSpec.description();
    }
}
