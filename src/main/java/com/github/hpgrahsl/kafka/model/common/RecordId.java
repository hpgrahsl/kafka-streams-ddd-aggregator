package com.github.hpgrahsl.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.hpgrahsl.kafka.model.custom.DefaultId;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.CLASS,
    include = JsonTypeInfo.As.PROPERTY,
    property = "_class")
@JsonSubTypes({
    @JsonSubTypes.Type(value = DefaultId.class)
})
public abstract class RecordId<T> {

    public abstract T getId();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

}
