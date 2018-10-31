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
package com.terracottatech.store.management;

import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.registry.AbstractManagementProvider;
import org.terracotta.management.registry.DefaultExposedObject;
import org.terracotta.management.registry.Named;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

@Named("DatasetSettings")
class DatasetSettingsManagementProvider extends AbstractManagementProvider<ManageableDataset<?>> {

  @SuppressWarnings("unchecked") private static final Class<ManageableDataset<?>> TYPE = (Class) ManageableDataset.class;

  private final String instanceId;

  DatasetSettingsManagementProvider(String instanceId) {
    super(TYPE);
    this.instanceId = Objects.requireNonNull(instanceId);
  }

  @Override
  public Collection<? extends Descriptor> getDescriptors() {
    Collection<Descriptor> descriptors = new ArrayList<>(super.getDescriptors());
    descriptors.add(new Settings()
        .set("type", "DatasetManager")
        .set("instanceId", instanceId));
    return descriptors;
  }

  @Override
  protected DefaultExposedObject<ManageableDataset<?>> wrap(ManageableDataset<?> managedObject) {
    return new DefaultExposedObject<ManageableDataset<?>>(managedObject, managedObject.getContext()) {
      @Override
      public Collection<? extends Descriptor> getDescriptors() {
        return Collections.singletonList(new Settings()
            .set("type", "Dataset")
            .set(ManageableDatasetManager.KEY_D_NAME, getTarget().getStatistics().getDatasetName())
            .set(ManageableDatasetManager.KEY_D_INST, getTarget().getStatistics().getInstanceName()));
      }
    };
  }

}
