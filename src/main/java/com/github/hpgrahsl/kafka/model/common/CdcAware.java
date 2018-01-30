package com.github.hpgrahsl.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.hpgrahsl.kafka.model.custom.Order;
import com.github.hpgrahsl.kafka.model.custom.OrderLine;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
    include = JsonTypeInfo.As.PROPERTY,
    property = "_class")
    @JsonSubTypes({
    @JsonSubTypes.Type(value = Order.class),
    @JsonSubTypes.Type(value = OrderLine.class)
})
public class CdcAware {

    private EventType eventType = EventType.UPSERT;

    public CdcAware(EventType eventType) {
        this.eventType = eventType == null ? EventType.UPSERT : eventType;
    }

    public EventType getEventType() {
        return eventType;
    }

}
