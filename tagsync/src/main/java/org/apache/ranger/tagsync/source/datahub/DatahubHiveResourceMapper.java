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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.tagsync.source.atlas.AtlasResourceMapper;

import java.util.HashMap;
import java.util.Map;

public class DatahubHiveResourceMapper extends DatahubResourceMapper {
	// public static final String DATASET_URN     = "urn:li:dataset";
	// public static final String ENTITY_TYPE_HIVE_TABLE  = "hive_table";
	// public static final String ENTITY_TYPE_HIVE_COLUMN = "hive_column";

	public static final String[] RANGER_TYPE_URI     = {"database", "table", "column"};

	public DatahubHiveResourceMapper() {
		super("hive");
	}

	@Override
	public RangerServiceResource buildResource(final DatahubEntity entity) throws Exception {
		String platform = (String)entity.getPlatform();
		if (StringUtils.isEmpty(platform)) {
			throw new Exception("attribute 'platform' not found in entity");
		}

		String   entityType  = entity.ge;
		String   serviceName = getRangerServiceName(clusterName);
		String[] resources   = resourceStr.split(QUALIFIED_NAME_DELIMITER);
		String   dbName      = resources.length > 0 ? resources[0] : null;
		String   tblName     = resources.length > 1 ? resources[1] : null;
		String   colName     = resources.length > 2 ? resources[2] : null;

		Map<String, RangerPolicyResource> elements = new HashMap<String, RangerPolicyResource>();

		if (StringUtils.equals(entityType, ENTITY_TYPE_HIVE_DB)) {
			if (StringUtils.isNotEmpty(dbName)) {
				elements.put(RANGER_TYPE_HIVE_DB, new RangerPolicyResource(dbName));
			}
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_HIVE_TABLE)) {
			if (StringUtils.isNotEmpty(dbName) && StringUtils.isNotEmpty(tblName)) {
				elements.put(RANGER_TYPE_HIVE_DB, new RangerPolicyResource(dbName));
				elements.put(RANGER_TYPE_HIVE_TABLE, new RangerPolicyResource(tblName));
			}
		} else if (StringUtils.equals(entityType, ENTITY_TYPE_HIVE_COLUMN)) {
			if (StringUtils.isNotEmpty(dbName) && StringUtils.isNotEmpty(tblName) && StringUtils.isNotEmpty(colName)) {
				elements.put(RANGER_TYPE_HIVE_DB, new RangerPolicyResource(dbName));
				elements.put(RANGER_TYPE_HIVE_TABLE, new RangerPolicyResource(tblName));
				elements.put(RANGER_TYPE_HIVE_COLUMN, new RangerPolicyResource(colName));
			}
		} else {
			throwExceptionWithMessage("unrecognized entity-type: " + entityType);
		}

		if(elements.isEmpty()) {
			throwExceptionWithMessage("invalid qualifiedName for entity-type '" + entityType + "': " + qualifiedName);
		}

		RangerServiceResource ret = new RangerServiceResource(serviceName, elements);

		return ret;
	}
}
