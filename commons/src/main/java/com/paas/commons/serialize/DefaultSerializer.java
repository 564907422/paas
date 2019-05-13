package com.paas.commons.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class DefaultSerializer extends Object
        implements Serializer<Serializable> {
    public byte[] serialize(Serializable serializable) throws SerializationException {
        if (serializable == null)
            return null;
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;

        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(serializable);
            return baos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException(e.getMessage(), e);
        }
    }


    public Serializable deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null)
            return null;
        ByteArrayInputStream bais = null;

        try {
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (Serializable) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException(e.getMessage(), e);
        }
    }
}
