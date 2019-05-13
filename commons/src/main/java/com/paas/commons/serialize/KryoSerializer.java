package com.paas.commons.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class KryoSerializer extends Object
        implements Serializer<Serializable> {
    private Kryo kryo;

    public KryoSerializer() {
        this.kryo = new Kryo();
        this.kryo.setReferences(false);
        this.kryo.setRegistrationRequired(false);
        this.kryo.setMaxDepth(20);
    }

    public KryoSerializer(int maxDepth) {
        this.kryo = new Kryo();
        this.kryo.setReferences(false);
        this.kryo.setRegistrationRequired(false);
        this.kryo.setMaxDepth((maxDepth < 1) ? 20 : maxDepth);
    }


    public Serializable deserialize(byte[] bytes) throws SerializationException {
        Input input = null;
        try {
            input = new Input(bytes);
            return (Serializable) this.kryo.readClassAndObject(input);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException(e.getMessage(), e);
        } finally {
            if (null != input) {
                input.close();
                input = null;
            }
        }
    }


    public byte[] serialize(Serializable serializable) throws SerializationException {
        ByteArrayOutputStream out = null;
        Output output = null;
        try {
            out = new ByteArrayOutputStream();
            output = new Output(out);
            this.kryo.writeClassAndObject(output, serializable);
            return output.toBytes();
        } catch (Exception e) {
            e.printStackTrace();
            throw new SerializationException(e.getMessage(), e);
        } finally {
            if (null != out) {
                try {
                    out.close();
                    out = null;
                } catch (IOException e) {
                    throw new SerializationException(e.getMessage(), e);
                }
            }
            if (null != output) {
                output.close();
                output = null;
            }
        }
    }
}

