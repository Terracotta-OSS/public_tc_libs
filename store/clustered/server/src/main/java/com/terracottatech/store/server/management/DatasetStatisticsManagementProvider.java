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
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.collect.StatisticProvider;
import org.terracotta.management.registry.collect.StatisticRegistry;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;
import org.terracotta.statistics.StatisticType;
import org.terracotta.statistics.Table;

import java.util.List;
import java.util.Map;

@Named("SovereignDatasetStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@StatisticProvider
@CommonComponent
public class DatasetStatisticsManagementProvider extends AbstractStatisticsManagementProvider<DatasetBinding> {
  public DatasetStatisticsManagementProvider() {
    super(DatasetBinding.class);
  }

  @Override
  protected AbstractExposedStatistics<DatasetBinding> internalWrap(Context context, DatasetBinding managedObject, StatisticRegistry statisticRegistry) {
    return new DatasetBindingExposedStatistics(context, managedObject, statisticRegistry);
  }

  private static class DatasetBindingExposedStatistics extends AbstractExposedStatistics<DatasetBinding> {
    DatasetBindingExposedStatistics(Context context, DatasetBinding binding, StatisticRegistry statisticRegistry) {
      super(context.with("type", "SovereignDataset"), binding, statisticRegistry);

      SovereignDataset<?> dataset = ((SovereignDataset<?>) binding.getDataset());

      getStatisticRegistry().registerGauge("SovereignDataset:AllocatedHeap", dataset::getAllocatedHeapStorageSize);
      getStatisticRegistry().registerGauge("SovereignDataset:AllocatedPersistentSupport", dataset::getAllocatedPersistentSupportStorage);
      getStatisticRegistry().registerGauge("SovereignDataset:AllocatedPrimaryKey", dataset::getAllocatedPrimaryKeyStorageSize);
      getStatisticRegistry().registerGauge("SovereignDataset:AllocatedIndex", dataset::getAllocatedIndexStorageSize);

      getStatisticRegistry().registerGauge("SovereignDataset:OccupiedHeap", dataset::getOccupiedHeapStorageSize);
      getStatisticRegistry().registerGauge("SovereignDataset:OccupiedPersistentSupport", dataset::getOccupiedPersistentSupportStorage);
      getStatisticRegistry().registerGauge("SovereignDataset:OccupiedPrimaryKey", dataset::getOccupiedPrimaryKeyStorageSize);

      getStatisticRegistry().registerGauge("SovereignDataset:Persistent", dataset::getPersistentBytesUsed);

      getStatisticRegistry().registerGauge("SovereignDataset:RecordCount", dataset::recordCount);

      getStatisticRegistry().registerGauge("SovereignDataset:AllocatedMemory", () ->
          dataset.getAllocatedHeapStorageSize()
              + dataset.getAllocatedPersistentSupportStorage()
              + dataset.getAllocatedIndexStorageSize()
              + dataset.getAllocatedPrimaryKeyStorageSize());

      getStatisticRegistry().registerGauge("SovereignDataset:MainRecordOccupiedStorage", () -> dataset.getAllocatedHeapStorageSize()
          + dataset.getOccupiedHeapStorageSize()
          + dataset.getOccupiedPersistentSupportStorage()
          + dataset.getOccupiedPrimaryKeyStorageSize());

      StreamTable streamTable = binding.getStreamTable();

      getStatisticRegistry().registerTable("SovereignDataset:StreamTable", () -> formatStreamsTable(streamTable.top(10)));

    }

    private Table formatStreamsTable(List<Map.Entry<StreamShape, StreamStatistics>> entries) {
      final String countRow = "Count";
      final String totalTimeRow = "TotalTime";
      final String serverTimeRow = "ServerTime";

      Table.Builder builder = Table.newBuilder(countRow, totalTimeRow, serverTimeRow);
      for (Map.Entry<StreamShape, StreamStatistics> row : entries) {
        builder.withRow(row.getKey().toString(), rb -> {
          rb.setStatistic(countRow, StatisticType.GAUGE, row.getValue().getExecutionCount());
          rb.setStatistic(totalTimeRow, StatisticType.GAUGE, row.getValue().getTotalTime());
          rb.setStatistic(serverTimeRow, StatisticType.GAUGE, row.getValue().getServerTime());
        });
      }
      return builder.build();
    }
  }
}
