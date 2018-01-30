package com.github.hpgrahsl.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Aggregate<P,C> {

    private final P parent;

    private final List<C> children;

    @JsonCreator
    public Aggregate(
            @JsonProperty("parent") P parent,
            @JsonProperty("children") List<C> children) {
        this.parent = parent;
        this.children = children;
    }

    public P getParent() {
        return parent;
    }

    public List<C> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return "Aggregate{" +
            "parent=" + parent +
            ", children=" + children +
            '}';
    }
}
