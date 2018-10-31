/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.terracottatech.tools.util;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.ConnectionPropertyNames;

import java.util.Properties;

public class ConnectionPropertiesUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPropertiesUtil.class);
  private static final String CLUSTER_TOOL_TIMEOUT_PROPERTY = "com.terracottatech.tools.clustertool.timeout";
  private static final String DEFAULT_TIMEOUT = "300000";

  public static Properties getConnectionProperties(Properties defaultProperties, String securityRootDirectory) {
    Properties connProperties = new Properties();
    if (defaultProperties != null) {
      connProperties.putAll(defaultProperties);
    }
    if (securityRootDirectory != null) {
      connProperties.setProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY, securityRootDirectory);
    }

    connProperties.setProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT, getConnectionTimeout());
    return connProperties;
  }

  public static String getConnectionTimeout() {
    String timeoutValue = System.getProperty(CLUSTER_TOOL_TIMEOUT_PROPERTY);
    if (parseTimeout(timeoutValue) <= 0) {
      LOGGER.debug("Found invalid value '{}' for property '{}'. Using default timeout of '{}' instead", timeoutValue, CLUSTER_TOOL_TIMEOUT_PROPERTY, DEFAULT_TIMEOUT);
      return DEFAULT_TIMEOUT;
    }
    return timeoutValue;
  }

  public static void setConnectionTimeout(String timeoutValue) {
    if (parseTimeout(timeoutValue) <= 0) {
      throw new IllegalArgumentException("Found invalid value '" + timeoutValue + "' for timeout. Timeout should be a positive number.");
    }
    System.setProperty("com.terracottatech.tools.clustertool.timeout", timeoutValue);
  }

  private static int parseTimeout(String timeoutValue) {
    int timeout = 0;
    try {
      timeout = Integer.parseInt(timeoutValue);
    } catch (NumberFormatException e) {
      //Ignore
    }
    return timeout;
  }
}