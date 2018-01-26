package com.github.hpgrahsl.kafka.serdes;

import com.github.hpgrahsl.kafka.model.common.CdcAware;
import com.github.hpgrahsl.kafka.model.common.EventType;
import org.apache.kafka.common.errors.SerializationException;

import java.util.HashMap;
import java.util.Map;

public class JsonDbzDeserializer<T> extends JsonPojoDeserializer<T> {

    public static final String DBZ_CDC_EVENT_PAYLOAD_FIELD = "payload";

    @Override
    public T deserialize(String topic, byte[] bytes) {

        if (bytes == null)
            return null;

        T data;

        try {
            Map temp = (Map)OBJECT_MAPPER.readValue(new String(bytes),Map.class)
                            .get(DBZ_CDC_EVENT_PAYLOAD_FIELD);
            if(temp == null) {
                temp = new HashMap();
                //NOTE: we record a delete event in this case
                if(CdcAware.class.isAssignableFrom(clazz)) {
                    temp.put("eventType", EventType.DELETE);
                }
            }
            temp.put("_class",clazz.getName());

            data = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(temp), clazz);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }


}
