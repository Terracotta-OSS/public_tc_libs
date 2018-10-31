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

package com.terracottatech.store.manager.config;

import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.manager.DatasetManagerConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedDatasetManagerConfiguration extends AbstractDatasetManagerConfiguration {

  private final ResourceConfiguration resourceConfiguration;

  private EmbeddedDatasetManagerConfiguration(Map<String, Long> offheapResources,
                                      Map<String, DiskResource> diskResources,
                                      Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets) {
    super(datasets);
    this.resourceConfiguration = new ResourceConfiguration(offheapResources, diskResources);
  }

  public ResourceConfiguration getResourceConfiguration() {
    return resourceConfiguration;
  }

  @Override
  public Type getType() {
    return Type.EMBEDDED;
  }

  public static class ResourceConfiguration {
    private final Map<String, Long> offheapResources = new HashMap<>();
    private final Map<String, DiskResource> diskResources = new HashMap<>();

    public ResourceConfiguration(Map<String, Long> offheapResources, Map<String, DiskResource> diskResources) {
      this.offheapResources.putAll(offheapResources);
      this.diskResources.putAll(diskResources);
    }

    public Map<String, DiskResource> getDiskResources() {
      return Collections.unmodifiableMap(diskResources);
    }

    public Map<String, Long> getOffheapResources() {
      return Collections.unmodifiableMap(offheapResources);
    }

    public EmbeddedDatasetManagerConfiguration withDatasetsConfiguration(Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets) {
      return new EmbeddedDatasetManagerConfiguration(offheapResources, diskResources, datasets);
    }

  }
}
