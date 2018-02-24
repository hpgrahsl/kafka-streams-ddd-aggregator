package com.github.hpgrahsl.kafka.serdes;

import com.github.hpgrahsl.kafka.model.EventType;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Map;

public class JsonHybridDeserializer<T> extends JsonPojoDeserializer<T> {

    public static final String DBZ_CDC_EVENT_PAYLOAD_FIELD = "payload";
    public static final String DBZ_CDC_EVENT_PAYLOAD_AFTER_FIELD = "after";
    public static final String DBZ_CDC_EVENT_PAYLOAD_BEFORE_FIELD = "before";

    @Override
    public T deserialize(String topic, byte[] bytes) {

        if (bytes == null)
            return null;


        //step1: try to directly deserialize expected class
        try {
            return OBJECT_MAPPER.readValue(bytes, clazz);
        } catch (IOException e1) {
            //step2: try to deserialize from DBZ JSON event with schema
            try {
                Map event = OBJECT_MAPPER.readValue(new String(bytes), Map.class);

                if(!event.containsKey(DBZ_CDC_EVENT_PAYLOAD_FIELD)
                        || event.get(DBZ_CDC_EVENT_PAYLOAD_FIELD) == null) {
                    throw new IllegalArgumentException("error: debezium event expected to contain"
                            + " non-null payload field (connector shall not send tombstone events)");
                }

                Map payload = (Map)event.get(DBZ_CDC_EVENT_PAYLOAD_FIELD);
                //check for DBZ key struct
                if(!payload.containsKey(DBZ_CDC_EVENT_PAYLOAD_BEFORE_FIELD)
                        && !payload.containsKey(DBZ_CDC_EVENT_PAYLOAD_AFTER_FIELD)) {
                    T data = OBJECT_MAPPER.readValue(
                            OBJECT_MAPPER.writeValueAsBytes(payload), clazz);
                    return data;
                }
                //check for DBZ value struct of upserts -> before field exists
                //and after field is available and non-null
                if(payload.containsKey(DBZ_CDC_EVENT_PAYLOAD_BEFORE_FIELD)
                        //&& payload.containsKey(DBZ_CDC_EVENT_PAYLOAD_AFTER_FIELD)
                        && payload.get(DBZ_CDC_EVENT_PAYLOAD_AFTER_FIELD) != null) {
                    T data = OBJECT_MAPPER.readValue(
                            OBJECT_MAPPER.writeValueAsBytes(
                                    payload.get(DBZ_CDC_EVENT_PAYLOAD_AFTER_FIELD)), clazz);
                    return data;
                }
                //check for DBZ value struct of deletes -> before field available and non-null
                //and after field exists
                if(//payload.containsKey(DBZ_CDC_EVENT_PAYLOAD_BEFORE_FIELD) &&
                        payload.get(DBZ_CDC_EVENT_PAYLOAD_BEFORE_FIELD) != null
                        && payload.get(DBZ_CDC_EVENT_PAYLOAD_AFTER_FIELD) == null) {
                    Map deletion = (Map)payload.get(DBZ_CDC_EVENT_PAYLOAD_BEFORE_FIELD);
                    deletion.put("_eventType", EventType.DELETE);
                    T data = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(deletion), clazz);
                    return data;
                }
            } catch(IOException e2) {
                throw new SerializationException(e2);
            }
            throw new SerializationException("error: could neither deserialize from pojo nor debezium event");
        }
    }


}
