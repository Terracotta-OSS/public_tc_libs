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
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexStatistics;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.collect.StatisticProvider;
import org.terracotta.management.registry.collect.StatisticRegistry;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;

@Named("SovereignIndexStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@StatisticProvider
@CommonComponent
public class DatasetIndexStatisticsManagementProvider extends AbstractStatisticsManagementProvider<DatasetIndexBinding> {
  public DatasetIndexStatisticsManagementProvider() {
    super(DatasetIndexBinding.class);
  }

  @Override
  protected AbstractExposedStatistics<DatasetIndexBinding> internalWrap(Context context, DatasetIndexBinding managedObject, StatisticRegistry statisticRegistry) {
    return new DatasetIndexBindingExposedStatistics(context, managedObject, statisticRegistry);
  }

  private static class DatasetIndexBindingExposedStatistics extends AbstractExposedStatistics<DatasetIndexBinding> {
    DatasetIndexBindingExposedStatistics(Context context, DatasetIndexBinding binding, StatisticRegistry statisticRegistry) {
      super(context
          .with("type", "SovereignIndex")
              .with("cellName", binding.getIndex().getDescription().getCellName())
              .with("cellType", binding.getIndex().getDescription().getCellType().getJDKType().getSimpleName())
              .with("indexSettings", binding.getIndex().getDescription().getIndexSettings().toString())
              .with("datasetName", binding.getValue().getDatasetName()),
          binding, statisticRegistry);

      SovereignIndex<?> index = binding.getIndex();
      SovereignIndexStatistics statistics = index.getStatistics();

      getStatisticRegistry().registerGauge("SovereignIndex:OccupiedStorage", statistics::occupiedStorageSize);
      getStatisticRegistry().registerGauge("SovereignIndex:IndexedRecordCount", statistics::indexedRecordCount);
      getStatisticRegistry().registerCounter("SovereignIndex:AccessCount", statistics::indexAccessCount);
    }
  }
}
