package org.apache.ranger.tagsync.source.datahub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DatahubEntity {
    private String urn;
    private String platform;
    private String service;
    private String name;
    private List<String> tableTags;
    private Map<String, List<String>> fieldTagMap;

    public String getUrn() {
        return urn;
    }

    public void setUrn(String urn) {
        this.urn = urn;
    }

    public String getPlatform() {
        return platform;
    }
    public String getService() {
        return service;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTableTags() {
        return tableTags;
    }

    public void setTableTags(List<String> tableTags) {
        this.tableTags = tableTags;
    }

    public Map<String, List<String>> getFieldTagMap() {
        return fieldTagMap;
    }

    public void setFieldTagMap(Map<String, List<String>> fieldTagMap) {
        this.fieldTagMap = fieldTagMap;
    }

    @JsonProperty("datasetKey")
    private void unpackDatasetKey(Map<String, Map<String, String>> fields) {
        String datasetUrn = fields.get("value").get("platform");
        if (!datasetUrn.startsWith("urn:li:")) {
            throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn);
        }
        this.platform =  datasetUrn.split(":")[2];
        this.service = datasetUrn.split(":")[3];
        this.name = fields.get("value").get("name");
    }

    @JsonProperty("editableSchemaMetadata")
    private void unpackEditableSchemaMetadata(Map<String, Map<String, Object>> schemaMetadata) {
        Map<String, Object> value = schemaMetadata.get("value");
        List<Map<String, Object>> editableSchemaFieldInfo = (List<Map<String, Object>>) value.get("editableSchemaFieldInfo");
        for (Map<String, Object> fieldInfo : editableSchemaFieldInfo) {
            if (fieldInfo.get("globalTags") != null && ((Map<String, Object>) fieldInfo.get("globalTags")).get("tags") != null) {
                String fieldPath = (String) fieldInfo.get("fieldPath");
                List<Map<String, String>> tags = (List<Map<String, String>>) ((Map<String, Object>) fieldInfo.get("globalTags")).get("tags");
                List<String> tagList = new ArrayList<>();
                for (Map<String, String> tag : tags) {
                    tagList.add(tag.get("tag"));
                }
                if (fieldTagMap == null) {
                    fieldTagMap = new HashMap<>();
                }
                fieldTagMap.put(fieldPath, tagList);
            }
        }
    }

    @JsonProperty("schemaMetadata")
    private void unpackSchemaMetadata(Map<String, Map<String, Object>> schemaMetadata) {
        Map<String, Object> value = schemaMetadata.get("value");
        List<Map<String, Object>> fields = (List<Map<String, Object>>) value.get("fields");
        for (Map<String, Object> field : fields) {
            if (field.get("globalTags") != null && ((Map<String, Object>) field.get("globalTags")).get("tags") != null) {
                String fieldPath = (String) field.get("fieldPath");
                List<Map<String, String>> tags = (List<Map<String, String>>) ((Map<String, Object>) field.get("globalTags")).get("tags");
                List<String> tagList = new ArrayList<>();
                for (Map<String, String> tag : tags) {
                    tagList.add(tag.get("tag"));
                }
                if (fieldTagMap == null) {
                    fieldTagMap = new HashMap<>();
                }
                fieldTagMap.put(fieldPath, tagList);
            }
        }
    }

    @JsonProperty("globalTags")
    private void unpackGlobalTags(Map<String, Map<String, Object>> globalTags) {
        List<Map<String, String>> tags = (List<Map<String, String>>) globalTags.get("value").get("tags");
        if (!tags.isEmpty() && tableTags == null) {
            tableTags = new ArrayList<>();
        }
        for (Map<String, String> tag : tags) {
            tableTags.add(tag.get("tag"));
        }
    }

    public Set<String> getAllTags() {
        Set<String> combinedTags = new HashSet<>();
        if (tableTags != null)
            combinedTags.addAll(tableTags);
        if (fieldTagMap != null)
            for (List<String> tags : fieldTagMap.values()) {
                combinedTags.addAll(tags);
            }
        return combinedTags;
    }
}

