package com.demo.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by yilong on 2017/5/10.
 */
public class AvroSerd {
    public static <V> byte[] serialize(final V object, final Class<V> cls) {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final Schema schema = ReflectData.get().getSchema(cls);
        final DatumWriter<V> writer = new GenericDatumWriter<V>(schema);
        final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        try {
            writer.write(object, binEncoder);
            binEncoder.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return bout.toByteArray();
    }

    public static <T> T deserialize(byte[] avroBytes, Class<T> clazz) {
        T ret = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(avroBytes);
            Decoder d = DecoderFactory.get().directBinaryDecoder(in, null);
            DatumReader<T> reader = new SpecificDatumReader<T>(clazz);
            ret = reader.read(null, d);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }
}
