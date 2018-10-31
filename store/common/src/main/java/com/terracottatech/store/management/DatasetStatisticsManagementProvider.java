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

import com.terracottatech.store.statistics.DatasetStatistics;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.DefaultStatisticsExposedObject;
import org.terracotta.management.registry.DefaultStatisticsManagementProvider;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.collect.StatisticProvider;
import org.terracotta.management.registry.collect.StatisticRegistry;

import java.util.function.LongSupplier;

@Named("DatasetStatistics")
@StatisticProvider
class DatasetStatisticsManagementProvider extends DefaultStatisticsManagementProvider<ManageableDataset<?>> {

  @SuppressWarnings("unchecked") private static final Class<ManageableDataset<?>> TYPE = (Class) ManageableDataset.class;

  DatasetStatisticsManagementProvider(Context parentContext, LongSupplier timeSource) {
    super(TYPE, timeSource, parentContext);
  }

  @Override
  protected DefaultStatisticsExposedObject<ManageableDataset<?>> wrap(ManageableDataset<?> managedObject) {
    DefaultStatisticsExposedObject<ManageableDataset<?>> exposedObject = new DefaultStatisticsExposedObject<>(managedObject, timeSource, managedObject.getContext());

    StatisticRegistry statisticRegistry = exposedObject.getStatisticRegistry();
    DatasetStatistics datasetStatistics = exposedObject.getTarget().getStatistics();
    managedObject.getStatistics().getKnownOutcomes().forEach(outcome -> {
      switch (outcome.getStatisticType()) {
        case COUNTER:
          statisticRegistry.registerCounter(outcome.getStatisticName(), () -> datasetStatistics.get(outcome));
          break;
        default:
          throw new IllegalArgumentException("Unsupported statistic type: " + outcome);
      }
    });
    managedObject.getStatistics().getDerivedStatistics().forEach(statisticRegistry::registerStatistic);

    return exposedObject;
  }

}
