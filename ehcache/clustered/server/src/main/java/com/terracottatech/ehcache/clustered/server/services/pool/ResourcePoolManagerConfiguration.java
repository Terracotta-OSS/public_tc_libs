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
package com.terracottatech.ehcache.clustered.server.services.pool;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.server.ResourcePoolManager;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.terracotta.entity.ServiceConfiguration;

/**
 * @author vmad
 */
@CommonComponent
public class ResourcePoolManagerConfiguration implements ServiceConfiguration<ResourcePoolManager> {

  private final String cacheManagerID;
  private final ServerSideConfiguration serverSideConfiguration;

  public ResourcePoolManagerConfiguration(String cacheManagerID) {
    this(cacheManagerID, null);
  }

  public ResourcePoolManagerConfiguration(String cacheManagerID, ServerSideConfiguration serverSideConfiguration) {
    this.cacheManagerID = cacheManagerID;
    this.serverSideConfiguration = serverSideConfiguration;
  }

  @Override
  public Class<ResourcePoolManager> getServiceType() {
    return ResourcePoolManager.class;
  }

  public String getCacheManagerID() {
    return cacheManagerID;
  }

  public ServerSideConfiguration getServerSideConfiguration() {
    return serverSideConfiguration;
  }
}
