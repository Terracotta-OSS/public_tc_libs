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
package com.terracottatech.ehcache.clustered.common;

import org.ehcache.clustered.common.ServerSideConfiguration;

import com.tc.classloader.CommonComponent;

import java.util.Map;

/**
 * Extends {@link ServerSideConfiguration} to add {@link RestartConfiguration}
 */
@CommonComponent
public class EnterpriseServerSideConfiguration extends ServerSideConfiguration {
  private static final long serialVersionUID = -2329731077194781813L;
  private RestartConfiguration restartConfiguration;

  public EnterpriseServerSideConfiguration(Map<String, Pool> resourcePools, RestartConfiguration restartConfiguration) {
    super(resourcePools);
    this.restartConfiguration = restartConfiguration;
  }

  public EnterpriseServerSideConfiguration(String defaultServerResource, Map<String, Pool> resourcePools,
                                           RestartConfiguration restartConfiguration) {
    super(defaultServerResource, resourcePools);
    this.restartConfiguration = restartConfiguration;
  }

  public RestartConfiguration getRestartConfiguration() {
    return restartConfiguration;
  }
}