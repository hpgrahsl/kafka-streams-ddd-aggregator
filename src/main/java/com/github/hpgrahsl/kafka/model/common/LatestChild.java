package com.github.hpgrahsl.kafka.model.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LatestChild<PK,FK,T extends CdcAware> {

    private RecordId<PK> childId;

    private RecordId<FK> parentId;

    private T latest;

    public LatestChild() {
    }

    @JsonCreator
    public LatestChild(
            @JsonProperty("childId") RecordId<PK> childId,
            @JsonProperty("parentId") RecordId<FK> parentId,
            @JsonProperty("latest") T latest) {
        this.childId = childId;
        this.parentId = parentId;
        this.latest = latest;
    }

    public void update(T child, RecordId<PK> childId, RecordId<FK> parentId) {
        if(EventType.DELETE == child.getEventType()) {
            latest = null;
            return;
        }
        latest = child;
        this.childId = childId;
        this.parentId = parentId;
    }

    public RecordId<PK> getChildId() {
        return childId;
    }

    public RecordId<FK> getParentId() {
        return parentId;
    }

    public T getLatest() {
        return latest;
    }

    @Override
    public String toString() {
        return "LatestChild{" +
            "childId=" + childId +
            ", parentId=" + parentId +
            ", latest=" + latest +
            '}';
    }
}
