package com.paas.commons.serialize;

import java.nio.charset.Charset;

public class StringSerializer
        extends Object
        implements Serializer<String> {
    private final Charset charset;

    public StringSerializer() {
        this(Charset.forName("UTF8"));
    }


    public StringSerializer(Charset charset) {
        this.charset = charset;
    }


    public String deserialize(byte[] bytes) {
        return (bytes == null) ? null : new String(bytes, this.charset);
    }


    public byte[] serialize(String string) {
        return (string == null) ? null : string.getBytes(this.charset);
    }
}
