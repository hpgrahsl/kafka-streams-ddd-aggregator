package com.github.hpgrahsl.kafka.model.custom;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.hpgrahsl.kafka.model.common.CdcAware;
import com.github.hpgrahsl.kafka.model.common.EventType;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Order extends CdcAware {

    private final int id;
    private final String ordernumber;
    private final Date orderdate;
    private final String orderstatus;

    @JsonCreator
    public Order(
        @JsonProperty("eventType") EventType eventType,
        @JsonProperty("id") int id,
        @JsonProperty("ordernumber") String ordernumber,
        @JsonProperty("orderdate") Date orderdate,
        @JsonProperty("orderstatus") String orderstatus) {
        super(eventType);
        this.id = id;
        this.ordernumber = ordernumber;
        this.orderdate = orderdate;
        this.orderstatus = orderstatus;
    }

    public int getId() {
        return id;
    }

    public String getOrdernumber() {
        return ordernumber;
    }

    public Date getOrderdate() {
        return orderdate;
    }

    public String getOrderstatus() {
        return orderstatus;
    }

    @Override
    public String toString() {
        return "Order{" +
            "eventType=" + getEventType() +
            ", id=" + id +
            ", ordernumber='" + ordernumber + '\'' +
            ", orderdate=" + orderdate +
            ", orderstatus='" + orderstatus + '\'' +
            '}';
    }
}
