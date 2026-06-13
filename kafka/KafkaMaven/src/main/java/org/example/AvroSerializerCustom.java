package org.example;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class AvroSerializerCustom<T extends SpecificRecord> implements Serializer<T>{
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, T t) {
        return new byte[0];
    }

    @Override
    public void close() {
        Serializer.super.close();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (data == null) return null;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            DatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

            writer.write(data, encoder);
            encoder.flush();

            byte[] bytes = outputStream.toByteArray();
            System.out.println("Serialized Bytes: " + Arrays.toString(bytes));

            return bytes;

        } catch (IOException e) {
            throw new RuntimeException("Error serializing Avro message", e);
        }
    }
}
