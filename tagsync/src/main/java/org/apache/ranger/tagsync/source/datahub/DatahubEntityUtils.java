package org.apache.ranger.tagsync.source.datahub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.util.ServiceTags;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;

public class DatahubEntityUtils {
    public static Map<String, Map<String, String>> DATAHUB_TO_RANGER_SERVICE_MAP = new HashMap<>();
    private static final Map<String, String> convertUriToTagCache = new HashMap<>();

    /**
     * Utility to convert datahubResponse object to RangerRequest(aka ServiceTags) object
     * @param dataset all dataste recieved from datahub which has tags
     * @return @Servicetags object with list of all dataset entries recieved from @DatahubResponse
     */
    static ServiceTags convertToRangerEntity(DatahubEntity dataset) {
        ServiceTags serviceTags = new ServiceTags();
        Map<Long, RangerTag> rangerTags = new HashMap<>();
        Map<Long, RangerTagDef> rangerTagDefs = new HashMap<>();

        // create ranger tags for all recieved datahub tags and add to rangerTags map, key for map should be autoincremented from 0
        int i = 0;
        for (String tag : dataset.getAllTags()) {
            // initialze tag
            RangerTag rangerTag = new RangerTag();
            rangerTag.setType(convertUriToTag(tag));
            rangerTag.setId((long) i);
            // initialize tagdef
            RangerTagDef rangerTagDef = new RangerTagDef();
            rangerTagDef.setId((long) i);
            rangerTagDef.setName(convertUriToTag(tag));
            rangerTagDef.setSource("datahub");
            rangerTagDef.setIsEnabled(true);


            rangerTags.put((long) i, rangerTag);
            rangerTagDefs.put((long) i, rangerTagDef);
            i++;
        }

        // create ranger service resource for each dataset and add to rangerServiceResources list
        serviceTags.setTags(rangerTags);
        serviceTags.setTagDefinitions(rangerTagDefs);
        serviceTags = setPlatformSpecificServiceResource(dataset, serviceTags);
        return serviceTags;
    }

    static void setServiceMapping(String customServiceMapping) {
        String[] datahubRangerServiceArray = StringUtils.tokenizeToStringArray(customServiceMapping, ",", true, false);
        while (datahubRangerServiceArray.length > 0) {
            // element 0 is key to DATAHUB_TO_RANGER_SERVICE_MAP, element 1 is key to value of DATAHUB_TO_RANGER_SERVICE_MAP and element 2 is value to DATAHUB_TO_RANGER_SERVICE_MAP
            if (datahubRangerServiceArray.length > 3) {
                if (!DATAHUB_TO_RANGER_SERVICE_MAP.containsKey(datahubRangerServiceArray[0])) {
                    DATAHUB_TO_RANGER_SERVICE_MAP.put(datahubRangerServiceArray[0], new HashMap<>());
                }
                DATAHUB_TO_RANGER_SERVICE_MAP.get(datahubRangerServiceArray[0]).put(datahubRangerServiceArray[1], datahubRangerServiceArray[2]);
            } else {
                throw new IllegalArgumentException("Invalid customServiceMapping: " + customServiceMapping);
            }
        }

    }
    /**
     * Create and set platform specific ServiceResource for RangerRequest.
     * As service resource and resource to tag id is tighly coupled, both are taken care in this.
     * and new object of type @ServiceTags is returned with all entries.
     * @param dataset datahubRequest object with dataest details
     * @param serviceTags Ranger request payload for adding service and their tag mapping.
     * @return  serviceTags @ServiceTags type with updated serviceResources and serviceToTagIds
     */
    static ServiceTags setPlatformSpecificServiceResource(DatahubEntity dataset, ServiceTags serviceTags) {
        List<RangerServiceResource> rangerServiceResources = null;
        Map<Long, List<Long>> resourceToTagIds = null;
        String platform = dataset.getPlatform();
        String service = dataset.getService();
        String rangerServiceName;
        if (!CollectionUtils.isEmpty(DatahubEntityUtils.DATAHUB_TO_RANGER_SERVICE_MAP) && !CollectionUtils.isEmpty(DatahubEntityUtils.DATAHUB_TO_RANGER_SERVICE_MAP.get(platform))) {
            rangerServiceName = DatahubEntityUtils.DATAHUB_TO_RANGER_SERVICE_MAP.get(platform).getOrDefault(service, null);
        } else {
            rangerServiceName = platform + "_" + service;
        }
        int i = 0;
        switch (service) {
            case "trino":
                rangerServiceResources = new ArrayList<>();
                resourceToTagIds = new HashMap<>();
                if (dataset.getTableTags() != null && !dataset.getTableTags().isEmpty()) {

                    RangerServiceResource rangerServiceResource = new RangerServiceResource();

                    rangerServiceResource.setResourceElements(new HashMap<>(
                            Map.of(
                                    "catalog", new RangerPolicy.RangerPolicyResource(dataset.getName().split("\\.")[0]),
                                    "schema", new RangerPolicy.RangerPolicyResource(dataset.getName().split("\\.")[1]),
                                    "table", new RangerPolicy.RangerPolicyResource(dataset.getName().split("\\.")[2])
                            )
                    ));
                    rangerServiceResource.setServiceName(rangerServiceName);
                    rangerServiceResource.setId((long) i);
                    rangerServiceResources.add(rangerServiceResource);
                    // match id from rangerTags key with tag name and add to resourceToTagIds map
                    List<Long> tagIds = new ArrayList<>();
                    for (String tag : dataset.getTableTags()) {
                        for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
                            if (entry.getValue().getType().equals(convertUriToTag(tag))) {
                                tagIds.add(entry.getKey());
                            }
                        }
                    }
                    resourceToTagIds.put((long) i++, tagIds);

                }
                if (dataset.getFieldTagMap() != null && !dataset.getFieldTagMap().isEmpty()) {
                    for (Map.Entry<String, List<String>> entry : dataset.getFieldTagMap().entrySet()) {
                        RangerServiceResource rangerServiceResource = new RangerServiceResource();
                        Map<String, RangerPolicy.RangerPolicyResource> resourceElementsMap = new HashMap<>();
                        resourceElementsMap.put("catalog", new RangerPolicy.RangerPolicyResource(dataset.getName().split("\\.")[0]));
                        resourceElementsMap.put("schema", new RangerPolicy.RangerPolicyResource(dataset.getName().split("\\.")[1]));
                        resourceElementsMap.put("table", new RangerPolicy.RangerPolicyResource(dataset.getName().split("\\.")[2]));
                        resourceElementsMap.put("column", new RangerPolicy.RangerPolicyResource(entry.getKey()));
                        rangerServiceResource.setResourceElements(resourceElementsMap);
                        rangerServiceResource.setServiceName(rangerServiceName);
                        rangerServiceResource.setId((long) i);
                        rangerServiceResources.add(rangerServiceResource);
                        // match id from rangerTags key with tag name in fieldTags and add to resourceToTagIds map
                        List<Long> tagIds = new ArrayList<>();
                        for (String tag : entry.getValue()) {
                            for (Map.Entry<Long, RangerTag> tagEntry : serviceTags.getTags().entrySet()) {
                                if (tagEntry.getValue().getType().equals(convertUriToTag(tag))) {
                                    tagIds.add(tagEntry.getKey());
                                }
                            }
                        }
                        resourceToTagIds.put((long) i++, tagIds);
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid platform: " + dataset.getPlatform());
        }
        serviceTags.setServiceResources(rangerServiceResources);
        serviceTags.setResourceToTagIds(resourceToTagIds);
        return serviceTags;
    }

    static String convertUriToTag(String tag) {
        if (convertUriToTagCache.containsKey(tag)) {
            return convertUriToTagCache.get(tag);
        }

        // Otherwise, compute the result
        if (tag.startsWith("urn:li:tag:")) {
            String result = tag.substring(11);
            // Store the result in the cache
            convertUriToTagCache.put(tag, result);
            return result;
        } else {
            throw new IllegalArgumentException("Invalid tag format: " + tag);
        }
    }

    public static void removeNullNodes(JsonNode node) {
        if (node.isArray()) {
            removeNullNodesFromArray((ArrayNode) node);
        } else if (node.isObject()) {
            removeNullNodesFromObject((ObjectNode) node);
        }
    }

    private static void removeNullNodesFromObject(ObjectNode node) {
        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();
            if (entry.getValue().isNull()) {
                iter.remove();
            } else {
                removeNullNodes(entry.getValue());
            }
        }
    }

    private static void removeNullNodesFromArray(ArrayNode node) {
        for (int i = 0; i < node.size(); i++) {
            removeNullNodes(node.get(i));
        }
    }
}
