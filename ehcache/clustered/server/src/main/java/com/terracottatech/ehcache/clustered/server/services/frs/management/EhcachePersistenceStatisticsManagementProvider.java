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
package com.terracottatech.ehcache.clustered.server.services.frs.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.frs.Statistics;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.collect.StatisticProvider;
import org.terracotta.management.registry.collect.StatisticRegistry;
import org.terracotta.management.service.monitoring.registry.provider.AbstractExposedStatistics;
import org.terracotta.management.service.monitoring.registry.provider.AbstractStatisticsManagementProvider;

@Named("EhcachePersistenceStatistics")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@StatisticProvider
@CommonComponent
public class EhcachePersistenceStatisticsManagementProvider extends AbstractStatisticsManagementProvider<RestartStoreBinding> {
  public EhcachePersistenceStatisticsManagementProvider() {
    super(RestartStoreBinding.class);
  }

  @Override
  protected AbstractExposedStatistics<RestartStoreBinding> internalWrap(Context context, RestartStoreBinding managedObject, StatisticRegistry statisticRegistry) {
    return new RestartStoreBindingExposedStatistics(context, managedObject, statisticRegistry);
  }

  private static class RestartStoreBindingExposedStatistics extends AbstractExposedStatistics<RestartStoreBinding> {
    RestartStoreBindingExposedStatistics(Context context, RestartStoreBinding binding, StatisticRegistry statisticRegistry) {
      super(context.with("type", "EhcacheRestartStore"), binding, statisticRegistry);
      Statistics statistics = binding.getValue().getStatistics();
      getStatisticRegistry().registerGauge("RestartStore:TotalUsage", statistics::getTotalUsed);
      getStatisticRegistry().registerGauge("RestartStore:ExpiredSize", statistics::getExpiredSize);
      getStatisticRegistry().registerGauge("RestartStore:LiveSize", statistics::getLiveSize);
      getStatisticRegistry().registerGauge("RestartStore:TotalAvailable", statistics::getTotalAvailable);
      getStatisticRegistry().registerGauge("RestartStore:TotalRead", statistics::getTotalRead);
      getStatisticRegistry().registerGauge("RestartStore:TotalWritten", statistics::getTotalWritten);
    }
  }
}
