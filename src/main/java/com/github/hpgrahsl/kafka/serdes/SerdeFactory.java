package com.github.hpgrahsl.kafka.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class SerdeFactory {

    public static <T> Serde<T> createPojoSerdeFor(Class<T> clazz, boolean isKey) {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("serializedClass",clazz);

        Serializer<T> ser = new JsonPojoSerializer<>();
        ser.configure(serdeProps,isKey);

        Deserializer<T> de = new JsonPojoDeserializer<>();
        de.configure(serdeProps,isKey);

        return Serdes.serdeFrom(ser,de);
    }

    public static <T> Serde<T> createDbzSerdeFor(Class<T> clazz, boolean isKey) {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("serializedClass",clazz);

        Serializer<T> ser = new JsonPojoSerializer<>();
        ser.configure(serdeProps,isKey);

        Deserializer<T> de = new JsonDbzDeserializer<>();
        de.configure(serdeProps,isKey);

        return Serdes.serdeFrom(ser,de);
    }

    public static <T> Serde<T> createHybridSerdeFor(Class<T> clazz, boolean isKey) {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("serializedClass",clazz);

        Serializer<T> ser = new JsonPojoSerializer<>();
        ser.configure(serdeProps,isKey);

        Deserializer<T> de = new JsonHybridDeserializer<>();
        de.configure(serdeProps,isKey);

        return Serdes.serdeFrom(ser,de);
    }

}
