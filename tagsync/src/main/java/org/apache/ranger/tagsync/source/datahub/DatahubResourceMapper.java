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
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class DatahubResourceMapper {
	private static final Logger LOG = LoggerFactory.getLogger(DatahubResourceMapper.class);

	public static final String URN_DELIMITER        = ":";
	public static final String TAGSYNC_DEFAULT_PLATFORM_AND_SERVICE_SEPARATOR = "_";
	public static final String TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX                  = "ranger.tagsync.datahub.";
	public static final String TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX                  = ".ranger.service";
	public static final String TAGSYNC_DATAHUB_PLATFORM_IDENTIFIER                        = ".instance.";


	protected Properties properties;
	protected String     defaultPlatformName = "dataPlatform";
	protected String     serviceName;

	public DatahubResourceMapper(String serviceName) {
		this.serviceName        = serviceName;
	}

	public final String getServiceName() {
		return serviceName;
	}


	public String getRangerServiceName(String platformName) {
		String ret = getCustomRangerServiceName(platformName);

		if (StringUtils.isBlank(ret)) {
			ret = platformName + TAGSYNC_DEFAULT_PLATFORM_AND_SERVICE_SEPARATOR + serviceName;
		}
		return ret;
	}

	public void initialize(Properties properties) {
		this.properties         = properties;
	}

	abstract public RangerServiceResource buildResource(final DatahubEntity entity) throws Exception;

	protected String getCustomRangerServiceName(String platformName) {
		if(properties != null) {
			String propName = TAGSYNC_SERVICENAME_MAPPER_PROP_PREFIX + platformName
					+ "." + serviceName
					+ TAGSYNC_SERVICENAME_MAPPER_PROP_SUFFIX;

			return properties.getProperty(propName);
		} else {
			return null;
		}
	}


	protected void throwExceptionWithMessage(String msg) throws Exception {
		LOG.error(msg);

		throw new Exception(msg);
	}
}
