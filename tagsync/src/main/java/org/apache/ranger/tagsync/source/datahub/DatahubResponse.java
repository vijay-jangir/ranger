package org.apache.ranger.tagsync.source.datahub;

import java.util.List;

public class DatahubResponse {
    private List<DatahubEntity> entities;
    private String scrollId;

    public List<DatahubEntity> getEntities() {
        return entities;
    }

    public void setEntities(List<DatahubEntity> entities) {
        this.entities = entities;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }
}
