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
 * KIND, either express or implied.  See the License for th
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.source.datahub;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DatahubResourceMapperUtil {
	private static final Logger LOG = LoggerFactory.getLogger(DatahubResourceMapperUtil.class);

	private static final Map<String, DatahubResourceMapper> datahubResourceMappers = new HashMap<String, DatahubResourceMapper>();

	public static boolean isEntityTypeHandled(String entityTypeName) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> isEntityTypeHandled(entityTypeName=" + entityTypeName + ")");
		}

		DatahubResourceMapper mapper = datahubResourceMappers.get(entityTypeName);

		boolean ret = mapper != null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== isEntityTypeHandled(entityTypeName=" + entityTypeName + ") : " + ret);
		}

		return ret;
	}

	public static RangerServiceResource getRangerServiceResource(DatahubEntity datahubEntity) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> getRangerServiceResource(" + datahubEntity.getService() +")");
		}

		RangerServiceResource resource = null;

		DatahubResourceMapper mapper = datahubResourceMappers.get(datahubEntity.getService());

		if (mapper != null) {
			try {
				resource = mapper.buildResource(datahubEntity);
			} catch (Exception exception) {
				LOG.error("Could not get serviceResource for datahub entity:" + datahubEntity.getPlatform() + ":" + datahubEntity.getService() + " :-> ", exception);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== getRangerServiceResource(" + datahubEntity.getService() +"): resource=" + resource);
		}

		return resource;
	}

	static public boolean initializeDatahubResourceMappers(Properties properties) {
		final String MAPPER_NAME_DELIMITER = ",";

		String customMapperNames = TagSyncConfig.getCustomDatahubResourceMappers(properties);

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> initializeDatahubResourceMappers.initializeDatahubResourceMappers(" + customMapperNames + ")");
		}
		boolean ret = true;

		List<String> mapperNames = new ArrayList<String>();
		mapperNames.add("org.apache.ranger.tagsync.source.datahub.DatahubHiveResourceMapper");
		mapperNames.add("org.apache.ranger.tagsync.source.datahub.DatahubTrinoResourceMapper");

		if (StringUtils.isNotBlank(customMapperNames)) {
			for (String customMapperName : customMapperNames.split(MAPPER_NAME_DELIMITER)) {
				mapperNames.add(customMapperName.trim());
			}
		}

		for (String mapperName : mapperNames) {
			try {
				Class<?> clazz = Class.forName(mapperName);
				DatahubResourceMapper resourceMapper = (DatahubResourceMapper) clazz.newInstance();

				resourceMapper.initialize(properties);

			} catch (Exception exception) {
				LOG.error("Failed to create DatahubResourceMapper:" + mapperName + ": ", exception);
				ret = false;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== initializeDatahubResourceMappers.initializeDatahubResourceMappers(" + mapperNames + "): " + ret);
		}
		return ret;
	}

	private static void add(String entityType, DatahubResourceMapper mapper) {
		datahubResourceMappers.put(entityType, mapper);
	}
}
