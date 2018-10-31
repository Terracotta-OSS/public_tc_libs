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
package com.terracottatech.store.server.state;

import com.tc.classloader.CommonComponent;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;

@CommonComponent
public class DatasetEntityStateServiceConfig<K extends Comparable<K>> implements ServiceConfiguration<DatasetEntityStateService<K>> {

  private final DatasetEntityConfiguration<K> entityConfiguration;
  private final ServiceRegistry serviceRegistry;

  public DatasetEntityStateServiceConfig(DatasetEntityConfiguration<K> entityConfiguration, ServiceRegistry serviceRegistry) {
    this.entityConfiguration = entityConfiguration;
    this.serviceRegistry = serviceRegistry;
  }

  public ServiceRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  public DatasetEntityConfiguration<K> getEntityConfiguration() {
    return entityConfiguration;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<DatasetEntityStateService<K>> getServiceType() {
    return (Class<DatasetEntityStateService<K>>) (Class) DatasetEntityStateService.class;
  }
}
