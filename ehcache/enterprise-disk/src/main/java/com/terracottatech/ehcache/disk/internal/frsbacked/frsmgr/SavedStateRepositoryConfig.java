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

import com.terracottatech.ehcache.common.frs.metadata.ChildConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;

import java.io.Serializable;

/**
 * State repository config stored in meta data store for each config.
 *
 * @author RKAV
 */
final class SavedStateRepositoryConfig<K, V> implements Serializable, ChildConfiguration<String> {

  private static final long serialVersionUID = -930668939159677201L;

  private int repoIndex;
  private final String storeId;
  private final FrsDataLogIdentifier dataLogId;
  private final Class<K> keyType;
  private final Class<V> valueType;

  SavedStateRepositoryConfig(String storeId, FrsDataLogIdentifier dataLogId,
                             final Class<K> keyType, final Class<V> valueType) {
    this.repoIndex = 0;
    this.storeId = storeId;
    this.dataLogId = dataLogId;
    this.keyType = keyType;
    this.valueType = valueType;
  }

  Class<K> getKeyType() {
    return keyType;
  }

  Class<V> getValueType() {
    return valueType;
  }

  public String getStoreId() {
    return storeId;
  }

  @Override
  public int getObjectIndex() {
    return repoIndex;
  }

  @Override
  public void setObjectIndex(int index) {
    this.repoIndex = index;
  }

  @Override
  public void validate(MetadataConfiguration newMetadataConfiguration) {
  }

  @Override
  public String getParentId() {
    return storeId;
  }

  @Override
  public FrsDataLogIdentifier getDataLogIdentifier() {
    return dataLogId;
  }
}