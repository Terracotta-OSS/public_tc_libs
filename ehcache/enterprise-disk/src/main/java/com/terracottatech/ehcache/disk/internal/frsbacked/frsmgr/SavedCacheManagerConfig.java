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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import com.terracottatech.ehcache.common.frs.metadata.FrsContainerIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.RootConfiguration;

import java.io.Serializable;
import java.util.Set;

/**
 * Cache manager configuration which is the parent metadata configuration
 *
 * @author RKAV
 */
final class SavedCacheManagerConfig implements RootConfiguration, Serializable {

  private static final long serialVersionUID = 8930066434054780816L;

  private int cacheManagerIndex;

  private final String cacheManagerAlias;
  private final Set<FrsContainerIdentifier> containerIds;

  SavedCacheManagerConfig(String cacheManagerAlias, Set<FrsContainerIdentifier> containerIds) {
    this.cacheManagerIndex = 0;
    this.cacheManagerAlias = cacheManagerAlias;
    this.containerIds = containerIds;
  }

  @Override
  public int getObjectIndex() {
    return cacheManagerIndex;
  }

  @Override
  public void setObjectIndex(int index) {
    this.cacheManagerIndex = index;
  }

  @Override
  public void validate(MetadataConfiguration newMetadataConfiguration) {
    if (!(newMetadataConfiguration instanceof SavedCacheManagerConfig)) {
      throw new IllegalArgumentException("Configuration type mismatch! Cache manager configuration expected");
    }
    SavedCacheManagerConfig newConfig = (SavedCacheManagerConfig)newMetadataConfiguration;
    if (!cacheManagerAlias.equals(newConfig.cacheManagerAlias)) {
      throw new IllegalArgumentException("Unexpected change in cache manager alias");
    }
  }

  @Override
  public Set<FrsContainerIdentifier> getContainers() {
    return containerIds;
  }
}