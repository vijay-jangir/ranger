/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.source.datahub;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.process.TagSynchronizer;
import org.apache.ranger.tagsync.source.atlas.AtlasNotificationMapper;
import org.apache.ranger.tagsync.source.atlas.AtlasResourceMapperUtil;
import org.apache.ranger.tagsync.source.atlas.EntityNotificationWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DatahubTagSource extends AbstractTagSource implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(DatahubTagSource.class);
    private final String DATASET_ENDPOINT_FOR_TAGS = "/openapi/v2/entity/dataset";
    private static final ObjectMapper mapper;

    static {
        new JsonMapper();
        mapper = JsonMapper.builder().enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION).build();
    }

    private long sleepTimeBetweenCycleInMillis;
	private String   restUrl          = null;
	private final boolean  isKerberized     = false;
    private String username = null;
    // private String password = null;
    private String token = null;
	private final String[] usernamePassword = null;
	private int      entitiesBatchSize = TagSyncConfig.DEFAULT_TAGSYNC_DATAHUB_SOURCE_ENTITIES_BATCH_SIZE;

	private Thread myThread = null;

	public static void main(String[] args) {

        DatahubTagSource datahubTagSource = new DatahubTagSource();

		TagSyncConfig config = TagSyncConfig.getInstance();

		Properties props = config.getProperties();

		TagSynchronizer.printConfigurationProperties(props);

        TagSink tagSink = TagSynchronizer.initializeTagSink(props);

        if (tagSink != null) {

            if (datahubTagSource.initialize(props)) {
                try {
                    tagSink.start();
                    datahubTagSource.setTagSink(tagSink);
                    datahubTagSource.synchUp();
                } catch (Exception exception) {
                    LOG.error("ServiceTags upload failed : ", exception);
                    System.exit(1);
                }
            } else {
                LOG.error("AtlasRESTTagSource initialization failed, exiting.");
                System.exit(1);
            }

        } else {
            LOG.error("TagSink initialization failed, exiting.");
            System.exit(1);
        }

	}
	@Override
	public boolean initialize(Properties properties) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasRESTTagSource.initialize()");
		}

		boolean ret = DatahubResourceMapperUtil.initializeDatahubResourceMappers(properties);

		sleepTimeBetweenCycleInMillis = TagSyncConfig.getTagSourceDatahubDownloadIntervalInMillis(properties);
		entitiesBatchSize = TagSyncConfig.getAtlasRestSourceEntitiesBatchSize(properties);

		String restEndpoint       = TagSyncConfig.getDatahubEndpoint(properties);
		String sslConfigFile = TagSyncConfig.getDatahubSslConfigFile(properties);
        this.username = TagSyncConfig.getDatahubUserName(properties);
        this.token = TagSyncConfig.getDatahubToken(properties);
        DatahubEntityUtils.setServiceMapping(TagSyncConfig.getDatahubToRangerServiceMap(properties));

		if (LOG.isDebugEnabled()) {
			LOG.debug("restUrl=" + restEndpoint);
			LOG.debug("sslConfigFile=" + sslConfigFile);
			LOG.debug("userName=" + username);
		}
        if (StringUtils.isNotEmpty(restEndpoint)) {
            this.restUrl = restEndpoint;
            if (restUrl.endsWith("/")) {
                restUrl = restUrl.substring(0, restUrl.length()-1);
            }

        } else {
			LOG.info("DatahubEndpoint not specified, Initial download of Atlas-entities cannot be done.");
			ret = false;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== DatahubTagSource.initialize(), result=" + ret);
		}

		return ret;
	}

	@Override
	public boolean start() {

		myThread = new Thread(this);
		myThread.setDaemon(true);
		myThread.start();

		return true;
	}

	@Override
	public void stop() {
		if (myThread != null && myThread.isAlive()) {
			myThread.interrupt();
		}
	}

	@Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRESTTagSource.run()");
        }
            while (true) {
                try {
                    if (TagSyncConfig.isTagSyncServiceActive()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("==> AtlasRESTTagSource.run() is running as server is Active");
                        }
                        synchUp();
                    }else{
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("==> This server is running passive mode");
                        }
                    }
                    LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");

                    Thread.sleep(sleepTimeBetweenCycleInMillis);

                } catch (InterruptedException exception) {
                LOG.error("Interrupted..: ", exception);
                return;
            } catch (Exception e) {
                LOG.error("Caught exception", e);
                return;
            }
        }
    }

	public void synchUp() throws Exception {
		DatahubResponse datahubEntities = getDatasetDetails();

		if (CollectionUtils.isNotEmpty(datahubEntities.getEntities())) {
			if (LOG.isDebugEnabled()) {
				for (DatahubEntity element : datahubEntities.getEntities()) {
					LOG.debug(Objects.toString(element));
				}
			}
            List<ServiceTags> tableResources = new ArrayList<>();
            for (DatahubEntity dataset : datahubEntities.getEntities()) {
                // convert DatahubDataset to RangerEntity
                ServiceTags serviceTags = DatahubEntityUtils.convertToRangerEntity(dataset);
                tableResources.add(serviceTags);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Time taken to convert datahub datasets to ranger entities: {} ms", System.currentTimeMillis() - startTime);
                LOG.debug("Number of Ranger entities prepared: {}", tableResources.size());
            }

            try {
                for (ServiceTags tableResource : tableResources) {
                    JsonNode node = mapper.valueToTree(tableResource);
                    DatahubEntityUtils.removeNullNodes(node);
                    String serviceTagsString = mapper.writeValueAsString(node);

					if (LOG.isDebugEnabled()) {
						Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z")
								.setPrettyPrinting()
								.create();
						LOG.debug("serviceTags=" + serviceTagsString);
					}
					updateSink(tableResource);
				}
			}
		}

	}

    private DatahubResponse getDatasetDetails() {
        String scrollId = null;
        DatahubResponse datasetResponse = new DatahubResponse();
        String datasetEndpoint = this.restUrl + DATASET_ENDPOINT_FOR_TAGS;
        HttpURLConnection connection = null;
        BufferedReader reader = null;

        try {
            do {
                URL url = new URL(datasetEndpoint);
                connection = (HttpURLConnection) url.openConnection();

                connection.setRequestMethod("GET");
                if (StringUtils.isNotEmpty(this.token)) {
                    connection.setRequestProperty("Authorization", "Bearer " + this.token);
                }

                if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;

                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }

                    DatahubResponse dataset = mapper.readValue(response.toString(), DatahubResponse.class);
                    if (datasetResponse.getEntities() != null) {
                        datasetResponse.getEntities().addAll(dataset.getEntities());
                    } else {
                        datasetResponse.setEntities(dataset.getEntities());
                    }
                    scrollId = dataset.getScrollId();
                    datasetEndpoint = this.restUrl + DATASET_ENDPOINT_FOR_TAGS + "&scrollId=" + scrollId;
                } else {
                    throw new IOException("Failed with response code: " + connection.getResponseCode());
                }
            } while (scrollId != null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get dataset details: " + e.getMessage() + ". Request sent: " + datasetEndpoint, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
        return datasetResponse;
    }
}

