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
package com.terracottatech.store.server.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.terracotta.management.service.monitoring.registry.provider.AliasBinding;

@CommonComponent
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class DatasetBinding extends AliasBinding {


  private final SovereignDataset<?> dataset;
  private final StreamTable streamTable;

  public DatasetBinding(SovereignDataset<?> dataset, StreamTable streamTable, DatasetEntityConfiguration<?> datasetEntityConfiguration) {
    super(datasetEntityConfiguration.getDatasetName(), datasetEntityConfiguration);
    this.dataset = dataset;
    this.streamTable = streamTable;
  }

  @Override
  public DatasetEntityConfiguration<?> getValue() {
    return (DatasetEntityConfiguration<?>) super.getValue();
  }

  public SovereignDataset<?> getDataset() {
    return dataset;
  }

  public StreamTable getStreamTable() {
    return streamTable;
  }
}
