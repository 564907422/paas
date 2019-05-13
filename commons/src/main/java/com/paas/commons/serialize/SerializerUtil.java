package com.paas.commons.serialize;

import java.io.Serializable;


public class SerializerUtil {
    private static KryoSerializer kryoSerializer = new KryoSerializer();

    private static StringSerializer stringSerializer = new StringSerializer();

    private static DefaultSerializer defaultSerializer = new DefaultSerializer();


    public static byte[] serialize(Serializable serializable) throws SerializationException {
        return kryoSerializer.serialize(serializable);
    }


    public static Serializable deserialize(byte[] bytes) throws SerializationException {
        return kryoSerializer.deserialize(bytes);
    }


    public static byte[] kryoSerialize(Serializable serializable) throws SerializationException {
        return kryoSerializer.serialize(serializable);
    }


    public static Serializable kryoDeserialize(byte[] bytes) throws SerializationException {
        return kryoSerializer.deserialize(bytes);
    }


    public static byte[] defaultSerialize(Serializable serializable) throws SerializationException {
        return defaultSerializer.serialize(serializable);
    }


    public static Serializable defaultDeserialize(byte[] bytes) throws SerializationException {
        return defaultSerializer.deserialize(bytes);
    }


    public static byte[] stringSerialize(String serializable) throws SerializationException {
        return stringSerializer.serialize(serializable);
    }


    public static String stringDeserialize(byte[] bytes) throws SerializationException {
        return stringSerializer.deserialize(bytes);
    }
}
