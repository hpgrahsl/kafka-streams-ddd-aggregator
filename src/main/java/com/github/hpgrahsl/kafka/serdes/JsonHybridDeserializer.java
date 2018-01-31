package com.github.hpgrahsl.kafka.serdes;

import com.github.hpgrahsl.kafka.model.common.CdcAware;
import com.github.hpgrahsl.kafka.model.common.EventType;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonHybridDeserializer<T> extends JsonPojoDeserializer<T> {

    public static final String DBZ_CDC_EVENT_PAYLOAD_FIELD = "payload";

    @Override
    public T deserialize(String topic, byte[] bytes) {

        if (bytes == null)
            return null;

        T data;

        //step1: try to directly deserialize expected class
        try {
            data = OBJECT_MAPPER.readValue(bytes, clazz);
        } catch (IOException e1) {
            //step2: try to deserialize from DBZ json event
            try {
                Map temp = (Map) OBJECT_MAPPER.readValue(new String(bytes), Map.class)
                        .get(DBZ_CDC_EVENT_PAYLOAD_FIELD);
                if (temp == null) {
                    temp = new HashMap();
                    //NOTE: we record a delete event in this case
                    if (CdcAware.class.isAssignableFrom(clazz)) {
                        temp.put("eventType", EventType.DELETE);
                    }
                }
                temp.put("_class", clazz.getName());

                data = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(temp), clazz);
            } catch(IOException e2) {
                throw new SerializationException(e2);
            }
        }

        return data;
    }


}
