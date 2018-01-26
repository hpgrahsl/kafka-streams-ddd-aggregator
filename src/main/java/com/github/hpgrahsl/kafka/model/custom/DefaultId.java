package com.github.hpgrahsl.kafka.model.custom;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.github.hpgrahsl.kafka.model.common.RecordId;

import java.io.IOException;

public class DefaultId extends RecordId<Integer> {

    @JsonIgnore
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static class CustomIdDeserializer extends KeyDeserializer {

        @Override
        public DefaultId deserializeKey(
                String key,
                DeserializationContext ctx) throws IOException {

            return OBJECT_MAPPER.readValue(key,DefaultId.class);
        }
    }

    public static class CustomIdSerializer extends JsonSerializer<DefaultId> {

        @Override
        public void serialize(DefaultId key,
                              JsonGenerator gen,
                              SerializerProvider serializers)
                throws IOException {

            gen.writeFieldName(OBJECT_MAPPER.writeValueAsString(key));
        }
    }

    private final Integer id;

    @JsonCreator
    public DefaultId(@JsonProperty("id") Integer id) {
        this.id = id;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultId defaultId = (DefaultId) o;

        return id != null ? id.equals(defaultId.id) : defaultId.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DefaultId{" +
            "id=" + id +
            '}';
    }

}
