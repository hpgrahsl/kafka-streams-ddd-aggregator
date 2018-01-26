package com.github.hpgrahsl.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.hpgrahsl.kafka.model.custom.DefaultId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Children<ID,T extends CdcAware> {

    @JsonProperty
    @JsonSerialize(keyUsing = DefaultId.CustomIdSerializer.class)
    @JsonDeserialize(keyUsing = DefaultId.CustomIdDeserializer.class)
    private Map<RecordId<ID>,T> entries = new LinkedHashMap<>();

    public void update(LatestChild<ID,?,T> child) {
        if(child.getLatest() != null) {
            entries.put(child.getChildId(),child.getLatest());
        } else {
            entries.remove(child.getChildId());
        }
    }

    @JsonIgnore
    public List<T> getEntries() {
        return new ArrayList<>(entries.values());
    }

    @Override
    public String toString() {
        return "Children{" +
            "entries=" + entries +
            '}';
    }
}
