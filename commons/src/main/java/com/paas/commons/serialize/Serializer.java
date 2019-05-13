package com.paas.commons.serialize;

public interface Serializer<T> {
    byte[] serialize(T paramT) throws SerializationException;

    T deserialize(byte[] paramArrayOfByte) throws SerializationException;
}