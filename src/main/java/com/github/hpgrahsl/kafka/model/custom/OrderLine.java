package com.github.hpgrahsl.kafka.model.custom;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.hpgrahsl.kafka.model.common.CdcAware;
import com.github.hpgrahsl.kafka.model.common.EventType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderLine extends CdcAware {

    private final int id;
    private final int order_id;
    private final String product;
    private final double price;
    private final int quantity;

    @JsonCreator
    public OrderLine(
        @JsonProperty("eventType") EventType eventType,
        @JsonProperty("id") int id,
        @JsonProperty("order_id") int order_id,
        @JsonProperty("product") String product,
        @JsonProperty("price") double price,
        @JsonProperty("quantity") int quantity) {
        super(eventType);
        this.id = id;
        this.order_id = order_id;
        this.product = product;
        this.price = price;
        this.quantity = quantity;
    }

    public int getId() {
        return id;
    }

    public int getOrder_id() {
        return order_id;
    }

    public String getProduct() {
        return product;
    }

    public double getPrice() {
        return price;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "OrderLine{" +
                "eventType=" + getEventType() +
                ", id=" + id +
                ", order_id=" + order_id +
                ", product='" + product + '\'' +
                ", price=" + price +
                ", quantity=" + quantity +
                '}';
    }
}
