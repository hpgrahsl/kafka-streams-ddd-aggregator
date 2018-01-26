package com.github.hpgrahsl.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Aggregate<P,C> {

    @JsonProperty
    private P parent;

    @JsonProperty
    private List<C> children;

    public Aggregate() {
    }

    public Aggregate(P parent, List<C> children) {
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
